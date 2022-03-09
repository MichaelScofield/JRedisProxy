package org.jrp.server.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import org.jrp.cmd.Command;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.IllegalCommandException;
import org.jrp.reply.Reply;
import org.jrp.server.RedisCommandsQueue;
import org.jrp.server.RedisServer;

@ChannelHandler.Sharable
public class RedisCommandAsyncHandler extends RedisCommandHandler {

    private final RedisCommandsQueue redisCommandsQueue;

    public RedisCommandAsyncHandler(RedisServer redisServer, ProxyConfig config) throws IllegalCommandException {
        super(redisServer, config);
        redisCommandsQueue = new RedisCommandsQueue(this);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        redisCommandsQueue.onChannelActive(ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        redisCommandsQueue.onChannelInactive(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    protected Reply executeCommand(Channel channel, Command cmd) {
        redisCommandsQueue.add(channel, cmd);
        return super.executeCommand(channel, cmd);
    }

    @Override
    protected void doRespond(Channel channel, Command command, Reply reply) {
        redisCommandsQueue.set(channel, command.id, reply);
        redisCommandsQueue.flushReadyReplies(channel);
    }
}
