package org.jrp.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.jrp.monitor.metrics.RedisproxyMetrics;
import org.jrp.reply.Reply;

public class RedisReplyEncoder extends MessageToByteEncoder<Reply> {

    @Override
    public void encode(ChannelHandlerContext ctx, Reply msg, ByteBuf out) {
        msg.write(out);

        RedisproxyMetrics.getCurrent().sent.incr();
        RedisproxyMetrics.getCurrent().bytesOut.incrBy(out.readableBytes());
    }
}
