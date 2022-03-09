package org.jrp.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.jrp.monitor.metrics.RedisproxyMetrics;
import org.jrp.reply.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisReplyEncoderTest {

    @Test
    public void testEncodeBulkReply() {
        String s = RandomStringUtils.randomAlphabetic(10);

        long sentBefore = RedisproxyMetrics.getCurrent().sent.get();
        long bytesOutBefore = RedisproxyMetrics.getCurrent().bytesOut.get();

        RedisReplyEncoder encoder = new RedisReplyEncoder();
        EmbeddedChannel channel = new EmbeddedChannel(encoder);
        channel.writeOutbound(BulkReply.bulkReply(s));
        channel.finish();
        ByteBuf reply = channel.readOutbound();

        assertEquals(String.format("$%d\r\n%s\r\n", s.length(), s), reply.toString(StandardCharsets.UTF_8));

        long sentAfter = RedisproxyMetrics.getCurrent().sent.get();
        long bytesOutAfter = RedisproxyMetrics.getCurrent().bytesOut.get();
        assertEquals(sentBefore + 1, sentAfter);
        assertEquals(bytesOutBefore + 5 + s.length() + 2, bytesOutAfter);
    }

    @Test
    public void testEncodeMultiBulkReply() {
        String s1 = RandomStringUtils.randomAlphabetic(10);
        String s2 = RandomStringUtils.randomAlphabetic(10);

        EmbeddedChannel channel = new EmbeddedChannel(new RedisReplyEncoder());
        channel.writeOutbound(MultiBulkReply.multiBulkReply(Arrays.asList(s1, s2)));
        channel.finish();
        ByteBuf reply = channel.readOutbound();
        assertEquals(
                String.format("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", s1.length(), s1, s2.length(), s2),
                reply.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodeAsyncReply() {
        String s = RandomStringUtils.randomAlphabetic(10);

        EmbeddedChannel channel = new EmbeddedChannel(new RedisReplyEncoder());
        channel.writeOutbound(new AsyncReply<>(() -> BulkReply.bulkReply(s)));
        channel.finish();
        ByteBuf reply = channel.readOutbound();

        assertEquals(String.format("$%d\r\n%s\r\n", s.length(), s), reply.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodeErrorReply() {
        String s = RandomStringUtils.randomAlphabetic(10);

        EmbeddedChannel channel = new EmbeddedChannel(new RedisReplyEncoder());
        channel.writeOutbound(new ErrorReply(s));
        channel.finish();
        ByteBuf reply = channel.readOutbound();

        assertEquals(String.format("-%s\r\n", s), reply.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodeIntegerReply() {
        long l = RandomUtils.nextLong(0, Long.MAX_VALUE);

        EmbeddedChannel channel = new EmbeddedChannel(new RedisReplyEncoder());
        channel.writeOutbound(new IntegerReply(l));
        channel.finish();
        ByteBuf reply = channel.readOutbound();

        assertEquals(String.format(":%s\r\n", l), reply.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testEncodeSimpleStringReply() {
        String s = RandomStringUtils.randomAlphabetic(10);

        EmbeddedChannel channel = new EmbeddedChannel(new RedisReplyEncoder());
        channel.writeOutbound(new SimpleStringReply(s));
        channel.finish();
        ByteBuf reply = channel.readOutbound();

        assertEquals(String.format("+%s\r\n", s), reply.toString(StandardCharsets.UTF_8));
    }
}