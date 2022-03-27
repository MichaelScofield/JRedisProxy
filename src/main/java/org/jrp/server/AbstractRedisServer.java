package org.jrp.server;

import io.lettuce.core.Range;
import io.lettuce.core.Range.Boundary;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.cmd.CommandProcessors;
import org.jrp.cmd.RedisKeyword;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.RedisException;
import org.jrp.monitor.ClientStat;
import org.jrp.reply.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import static org.jrp.exception.RedisException.NOT_IMPLEMENTED_ERROR;
import static org.jrp.reply.SimpleStringReply.OK;
import static org.jrp.reply.SimpleStringReply.RESET;
import static org.jrp.utils.BytesUtils.*;

public abstract class AbstractRedisServer implements RedisServer {

    private static final Logger LOGGER = LogManager.getLogger(AbstractRedisServer.class);

    final ProxyConfig proxyConfig;

    public AbstractRedisServer(ProxyConfig proxyConfig) {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public ProxyConfig getProxyConfig() {
        return proxyConfig;
    }

    @Override
    public final Reply select(byte[] index) {
        int db = toInt(index);
        ClientStat stat = ClientStat.getStat(RedisServerContext.getChannel());
        stat.setDb(db);
        return OK;
    }

    @Override
    public final Reply client(byte[][] args) {
        ClientStat stat = ClientStat.getStat(RedisServerContext.getChannel());
        switch (RedisKeyword.convert(args[0])) {
            case LIST:
                // TODO Handle more "CLIENT LIST" options.
                LOGGER.warn("start handling \"CLIENT LIST\" command from {}",
                        RedisServerContext.getCommand().getClientAddress());
                return new AsyncReply<>(() -> BulkReply.bulkReply(ClientStat.list()));
            case GETNAME:
                return BulkReply.bulkReply(stat.getName());
            case SETNAME:
                String clientName = string(args[1]);
                stat.setName(clientName);
                return SimpleStringReply.OK;
            case ID:
                return IntegerReply.integer(stat.id);
            case INFO:
                return BulkReply.bulkReply(stat.dump());
            case KILL:
                // TODO Implement "CLIENT KILL" command.
            case PAUSE:
            case UNPAUSE:
                // TODO Implement "CLIENT PAUSE and UNPAUSE" command.
            case REPLY:
                // TODO Implement "CLIENT REPLY" command.
            case UNBLOCK:
                // TODO Implement "CLIENT UNBLOCK" command.
            case CACHING:
            case GETREDIR:
            case TRACKING:
            case TRACKINGINFO:
                // TODO How to implement Redis client side caching related commands?
                //  ref: https://redis.io/topics/client-side-caching
            default:
                return ErrorReply.NOT_IMPL;
        }
    }

    @Override
    public final Reply config(byte[][] args) {
        return switch (RedisKeyword.convert(args[0])) {
            case GET -> configGet(args);
            case SET -> configSet(args);
            case RESETSTAT -> configResetstat();
            case REWRITE -> configRewrite();
            default -> ErrorReply.SYNTAX_ERROR;
        };
    }

    private Reply configGet(byte[][] args) throws RedisException {
        String parameter = string(args[1]);
        if (parameter.startsWith("proxy.")) {
            String proxyParameter = StringUtils.removeStart(parameter, "proxy.");
            String value;
            try {
                value = BeanUtils.getProperty(getProxyConfig(), proxyParameter);
            } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new RedisException(e);
            }
            return MultiBulkReply.from(Arrays.asList(parameter, value));
        } else {
            return doConfigGet(parameter);
        }
    }

    protected Reply doConfigGet(String parameter) {
        return ErrorReply.NOT_IMPL;
    }

    // TODO Select some unchangeable proxy configs such as "port" or "name"
    //  to return Redis error "ERR Unsupported CONFIG parameter: xxx"
    private Reply configSet(byte[][] args) throws RedisException {
        String parameter = string(args[1]);
        String value = string(args[2]);
        if (parameter.startsWith("proxy.")) {
            String proxyParameter = StringUtils.removeStart(parameter, "proxy.");
            if (proxyParameter.equals("timeout")) {
                int proxyTimeout = Integer.parseInt(value);
                ClientStat stat = ClientStat.getStat(RedisServerContext.getChannel());
                stat.setProxyTimeout(proxyTimeout);
            } else {
                try {
                    BeanUtils.setProperty(getProxyConfig(), parameter, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RedisException(e);
                }
            }
            return SimpleStringReply.OK;
        } else {
            return doConfigSet(parameter, value);
        }
    }

    protected Reply doConfigSet(String parameter, String value) {
        return ErrorReply.NOT_IMPL;
    }

    protected Reply configResetstat() {
        return ErrorReply.NOT_IMPL;
    }

    protected Reply configRewrite() {
        return ErrorReply.NOT_IMPL;
    }

    @Override
    public Reply info(byte[] section) {
        String sb = "# Proxy Config\n" + getProxyConfig() + "\n" + doInfo(section) + "\n";
        return BulkReply.bulkReply(sb);
    }

    protected String doInfo(byte[] section) {
        return null;
    }

    @Override
    public Reply failover(byte[][] options) {
        // TODO Implement "FAILOVER" command: https://redis.io/commands/failover
        return ErrorReply.NOT_IMPL;
    }

    @Override
    public final Reply command(byte[] subcommand, byte[][] options) {
        if (subcommand == null) {
            // TODO Implement the "COMMAND" command: https://redis.io/commands/command (after everything has done).
            return ErrorReply.NOT_IMPL;
        }
        return switch (RedisKeyword.convert(subcommand)) {
            case COUNT -> IntegerReply.integer(CommandProcessors.count());
            // TODO Implement all these "COMMAND" subcommands.
            case DOCS, GETKEYS, GETKEYSANDFLAGS, INFO, LIST -> throw NOT_IMPLEMENTED_ERROR;
            default -> ErrorReply.SYNTAX_ERROR;
        };
    }

    @Override
    public final Reply reset() {
        // TODO Implement real reset: https://redis.io/commands/reset
        return RESET;
    }

    Range<Number> createRange(byte[] min, byte[] max) {
        return Range.from(createBoundary(min), createBoundary(max));
    }

    private Boundary<Number> createBoundary(byte[] bytes) {
        Boundary<Number> boundary = Boundary.unbounded();
        if (bytes != null && bytes.length > 0) {
            boundary = bytes[0] == '(' ?
                    Boundary.excluding(toDouble(bytes, 1, bytes.length - 1)) :
                    Boundary.including(toDouble(bytes));
        }
        return boundary;
    }
}
