package org.jrp.server.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.cmd.*;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.IllegalCommandException;
import org.jrp.exception.RedisException;
import org.jrp.monitor.ClientStat;
import org.jrp.monitor.CommandTimeMonitor;
import org.jrp.monitor.Monitor;
import org.jrp.monitor.counter.LongCounter;
import org.jrp.monitor.metrics.CmdLifecycleMetrics;
import org.jrp.monitor.metrics.RedisproxyMetrics;
import org.jrp.monitor.metrics.RedisproxyStatusMetrics;
import org.jrp.reply.AsyncReply;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.FutureReply;
import org.jrp.reply.Reply;
import org.jrp.server.RedisServer;
import org.jrp.server.RedisServerContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.jrp.cmd.CommandLifecycle.STATE.FINALIZE;
import static org.jrp.cmd.CommandLifecycle.STATE.NEW;
import static org.jrp.reply.ErrorReply.NIL_REPLY;
import static org.jrp.reply.SimpleStringReply.QUIT;
import static org.jrp.utils.BytesUtils.bytes;

@ChannelHandler.Sharable
public class RedisCommandHandler extends SimpleChannelInboundHandler<Command> {

    private static final Logger LOGGER = LogManager.getLogger(RedisCommandHandler.class);
    private static final ThreadPoolExecutor ASYNC_CMD_EXECUTOR = new ThreadPoolExecutor(
            0, 1,
            10, TimeUnit.MINUTES,
            new SynchronousQueue<>(),
            new BasicThreadFactory.Builder().namingPattern("AsyncCommandExecutor-%d").build(),
            (r, e) -> {
                if (!e.isShutdown()) {
                    LOGGER.warn("AsyncCommandExecutor is busy, execute command " + r + " synchronously.");
                    r.run();
                }
            });

    private final RedisServer redisServer;
    private final ProxyConfig proxyConfig;

    public RedisCommandHandler(RedisServer redisServer, ProxyConfig config) throws IllegalCommandException {
        this.redisServer = redisServer;
        this.proxyConfig = config;

        Map<String, String> renameCommands = config.getRenameCommands();
        if (renameCommands != null) {
            processRenameCommands(renameCommands);
        }
    }

    private void processRenameCommands(Map<String, String> renameCommands) throws IllegalCommandException {
        for (Map.Entry<String, String> entry : renameCommands.entrySet()) {
            String originCommand = entry.getKey();
            CommandProcessor command = CommandProcessors.remove(bytes(originCommand));

            String renameCommand = entry.getValue();
            if (StringUtils.isNotBlank(renameCommand)) {
                CommandProcessor newCommand = new CommandProcessor(renameCommand, command.getCommandMethod());
                CommandProcessors.add(newCommand);
                LOGGER.info("rename command " + originCommand + " to " + renameCommand);
            } else {
                LOGGER.info("remove command " + originCommand);
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            ctx.close();
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command cmd) {
        RedisproxyStatusMetrics.INSTANCE.queued.decr();
        RedisproxyStatusMetrics.INSTANCE.threads.incr();

        Channel channel = ctx.channel();
        RedisServerContext.fill(cmd, channel);
        try {
            Reply reply = handleCommand(channel, cmd);
            if (reply != null) {
                handleReply(channel, cmd, reply);
            }
        } finally {
            RedisServerContext.clear();

            RedisproxyStatusMetrics.INSTANCE.threads.decr();
        }
    }

    private Reply handleCommand(Channel channel, Command cmd) {
        if (!setRemoteAddress(channel, cmd)) {
            return null;
        }

        if (Monitor.hasMonitor()) {
            // TODO get db and remote address from client stat
            Monitor.record(0, cmd.getClientAddress(), cmd.toString());
        }

        int proxyTimeout = ClientStat.getStat(channel).getProxyTimeout();
        if (proxyTimeout > 0) {
            long cmdQueuedTime = cmd.getCommandLifecycle().getTimeCostsFromNowInMicroseconds(NEW);
            if (cmdQueuedTime / 1000 > proxyTimeout) {
                RedisproxyMetrics.getCurrent().dropCmd.incr();
                return null;
            }
        }

        if (cmd instanceof InvalidCommand invalidCommand) {
            RedisproxyMetrics.getCurrent().errProtocol.incr();

            LOGGER.warn("received invalid command " + invalidCommand.toShortString() + ", " +
                    "stacktrace is:", invalidCommand.getCause());
            return new ErrorReply(ExceptionUtils.getRootCauseMessage(invalidCommand.getCause()));
        }

        return executeCommand(channel, cmd);
    }

    private boolean setRemoteAddress(Channel channel, Command cmd) {
        SocketAddress socket = channel.remoteAddress();
        if (socket == null) {
            // According to 'remoteAddress' javadoc, we can ...
            LOGGER.warn("Discard command " + cmd.toShortString() + " because remote channel is not connected.");
            RedisproxyMetrics.getCurrent().dropCmd.incr();
            return false;
        }
        if (socket instanceof InetSocketAddress inetSocket) {
            String remoteAddress = inetSocket.getAddress().getHostAddress();
            int remotePort = inetSocket.getPort();
            cmd.setClientAddress(remoteAddress + ":" + remotePort);
        } else {
            cmd.setClientAddress(socket.toString());
        }
        return true;
    }

    protected Reply executeCommand(Channel channel, Command cmd) {
        CommandProcessor processor = cmd.getCommandProcessor();
        if (processor == null) {
            return new ErrorReply("unknown command " + cmd.toPrettyString());
        }

        RedisproxyMetrics metrics = RedisproxyMetrics.getCurrent();
        LongCounter counter = switch (processor.getRWType()) {
            case READ -> metrics.procRead;
            case WRITE -> metrics.procWrite;
            case OTHER -> metrics.procOther;
        };
        counter.incr();

        Reply reply;
        if (processor.getRWType() == RWType.Type.WRITE && proxyConfig.isReadOnly()) {
            reply = ErrorReply.READONLY_ERROR;
        } else {
            try {
                reply = processor.execute(cmd, redisServer);
            } catch (RedisException e) {
                LOGGER.error("unable to handle command " + cmd.toShortString(), e);
                reply = new ErrorReply(String.format(
                        "unable to handle command %s, error: %s",
                        cmd.toPrettyString(), ExceptionUtils.getRootCauseMessage(e)));
            }
        }
        return reply == null ? NIL_REPLY : reply;
    }

    private void handleReply(Channel channel, Command cmd, Reply reply) {
        if (reply instanceof AsyncReply asyncReply) {
            ASYNC_CMD_EXECUTOR.execute(() -> {
                Thread thread = Thread.currentThread();
                String threadName = thread.getName();
                try {
                    thread.setName(threadName + "(" + cmd.toShortString() + ")@" + channel);
                    asyncReply.getReply();
                    respond(channel, cmd, asyncReply);
                } catch (Throwable e) {
                    LOGGER.error("unable to handle command " + cmd.toShortString(), e);
                    ErrorReply errorReply = new ErrorReply(String.format(
                            "unable to handle command %s, error: %s",
                            cmd.toPrettyString(), ExceptionUtils.getRootCauseMessage(e)));
                    respond(channel, cmd, errorReply);
                } finally {
                    thread.setName(threadName);
                }
            });
        } else if (reply instanceof FutureReply futureReply) {
            //noinspection unchecked
            futureReply.onComplete((r) -> respond(channel, cmd, (Reply) r));
        } else {
            respond(channel, cmd, reply);
        }
    }

    private void respond(Channel channel, Command command, Reply reply) {
        recordCommandExecution(command);
        doRespond(channel, command, reply);
    }

    protected void doRespond(Channel channel, Command command, Reply reply) {
        flush(channel, command, reply);
    }

    private void recordCommandExecution(Command cmd) {
        cmd.getCommandLifecycle().setState(FINALIZE);

        long elapsed = cmd.getCommandLifecycle().getTimeCostsBetweenInMicroseconds(NEW, FINALIZE);
        if (elapsed > proxyConfig.getSlowlogLogSlowerThan()) {
            RedisproxyMetrics.getCurrent().slowExec.incr();
        }

        CommandProcessor processor = cmd.getCommandProcessor();
        String commandName = processor == null ? "unknown" : processor.getCommandName();
        CommandTimeMonitor.getCurrent().addTimeCost(commandName, (int) elapsed);

        CmdLifecycleMetrics.getCurrent().record(cmd);
    }

    public void flush(Channel channel, Command command, Reply reply) {
        if (channel.isActive()) {
            if (channel.isWritable()) {
                channel.writeAndFlush(reply).addListener(future -> {
                    if (!future.isSuccess()) {
                        Throwable cause = future.cause();
                        if (cause instanceof ClosedChannelException) {
                            LOGGER.warn("Remote channel {} was closed when trying to write reply {}",
                                    channel, reply);
                        } else if (cause instanceof IOException) {
                            if (cause.getMessage().contains("Connection reset by peer")) {
                                RedisproxyMetrics.getCurrent().connReset.incr();
                            } else {
                                LOGGER.warn("IOException {} for channel {}",
                                        ExceptionUtils.getRootCauseMessage(cause), channel);
                            }
                        } else {
                            LOGGER.error("caught unexpected exception for channel " + channel, cause);
                        }
                    }
                });
            } else {
                RedisproxyMetrics.getCurrent().dropReply.incr();
            }
        } else {
            RedisproxyMetrics.getCurrent().dropReply.incr();
        }
        if (reply == QUIT) {
            if (Monitor.hasMonitor()) {
                Monitor.stopMonitor(channel);
            }
            channel.close();
        }
        if (command instanceof InvalidCommand) {
            channel.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error("Caught exception for channel " + ctx.channel(), cause);
        ctx.channel().writeAndFlush(new ErrorReply(ExceptionUtils.getRootCauseMessage(cause)));
    }
}
