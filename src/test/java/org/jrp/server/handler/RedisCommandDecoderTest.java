package org.jrp.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.jrp.cmd.Command;
import org.jrp.exception.RedisCodecException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.jrp.server.handler.RedisCommandDecoder.readNum;
import static org.jrp.utils.BytesUtils.bytes;
import static org.junit.jupiter.api.Assertions.*;

public class RedisCommandDecoderTest {

    @Test
    public void testReadNum() throws RedisCodecException {
        assertEquals(0, readNum(Unpooled.wrappedBuffer(bytes("-0\r\n"))));
        assertEquals(-1, readNum(Unpooled.wrappedBuffer(bytes("-1\r\n"))));
        for (int i = 0; i < 100; i++) {
            int r = RandomUtils.nextInt(0, Integer.MAX_VALUE);
            assertEquals(r, readNum(Unpooled.wrappedBuffer(bytes(r + "\r\n"))));
        }

        assertEquals(0, readNum(Unpooled.wrappedBuffer(bytes("0\r\n"))));
        assertEquals(1, readNum(Unpooled.wrappedBuffer(bytes("1\r\n"))));
        for (int i = 0; i < 100; i++) {
            int r = -RandomUtils.nextInt(0, Integer.MAX_VALUE);
            assertEquals(r, readNum(Unpooled.wrappedBuffer(bytes(r + "\r\n"))));
        }
    }

    @Test
    public void testReadIllegalNum() {
        RedisCodecException e1 = assertThrows(RedisCodecException.class, () ->
                readNum(Unpooled.wrappedBuffer(bytes("100\r"))));
        assertEquals("expect '\\n' after '\\r' as RESP delimiter, actual EOF", e1.getMessage());

        RedisCodecException e2 = assertThrows(RedisCodecException.class, () ->
                readNum(Unpooled.wrappedBuffer(bytes("100\ra"))));
        assertEquals("expect '\\n' after '\\r' as RESP delimiter, actual 97", e2.getMessage());

        RedisCodecException e3 = assertThrows(RedisCodecException.class, () ->
                readNum(Unpooled.wrappedBuffer(bytes("1a0\r\n"))));
        assertEquals("expect got number to be parsed, actual got 'a'", e3.getMessage());
    }

    @Test
    public void testDecodeBulkString() {
        RedisCommandDecoder decoder = new RedisCommandDecoder();

        String s1 = RandomStringUtils.randomAlphabetic(10);
        String s2 = RandomStringUtils.randomAlphabetic(10);
        String largeBulkString = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1048576 + 1, 1048576 * 2));
        List<String> expects = Arrays.asList(
                String.format("\"SET\" \"%s\" \"%s\"(unknown, NEW)", s1, s2),
                String.format("InvalidCommand{cause=RedisCodecException: " +
                        "Expecting '$' as bulk string start, got '%s'" +
                        "}", s1.charAt(0)),
                String.format("InvalidCommand{cause=RedisCodecException: " +
                        "Bulk string size %s is not valid, must in range [0, 1048576)" +
                        "}", largeBulkString.length()),
                String.format("InvalidCommand{cause=RedisCodecException: " +
                        "Expecting %s bytes before \\r, actual %d" +
                        "}", s1.length() - 1, s1.length()));
        List<byte[]> byteBufs = Arrays.asList(
                bytes(String.format("$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", s1.length(), s1, s2.length(), s2)),
                bytes(s1),
                bytes(String.format("$%d\r\n%s\r\n", largeBulkString.length(), largeBulkString)),
                bytes(String.format("$%d\r\n%s\r\n", s1.length() - 1, s1)));

        for (int i = 0; i < expects.size(); i++) {
            decoder.setBulkStrings(new byte[3][]);

            ByteBuf in = Unpooled.wrappedBuffer(byteBufs.get(i));
            Command command = decoder.decodeBulkStrings(in);
            assertEquals(expects.get(i), command.toString());

            assertEquals(0, in.readableBytes());
            assertNull(decoder.getBulkStrings());
            assertEquals(0, decoder.getCurrBulkStringIndex());
        }
    }

    @Test
    public void testDecodeArray() throws RedisCodecException {
        RedisCommandDecoder decoder = new RedisCommandDecoder();

        ByteBuf in1 = Unpooled.wrappedBuffer(bytes("*3\r\n"));
        decoder.decodeArray(in1);
        assertEquals(3, decoder.getBulkStrings().length);
        assertEquals(0, decoder.getCurrBulkStringIndex());

        int i = RandomUtils.nextInt(1048576 + 1, 1048576 * 2);
        ByteBuf in2 = Unpooled.wrappedBuffer(bytes(String.format("*%d\r\n", i)));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> decoder.decodeArray(in2));
        assertEquals(
                String.format("Array length %s is not valid, must in range [0, 1048576)", i),
                e.getMessage());
    }

    @Test
    public void testDecodeInline() {
        RedisCommandDecoder decoder = new RedisCommandDecoder();
        String s1 = RandomStringUtils.randomAlphabetic(10);
        String s2 = RandomStringUtils.randomAlphabetic(10);
        ByteBuf in = Unpooled.wrappedBuffer(bytes(String.format("SET %s  %s\r\n", s1, s2)));
        Command command = decoder.decodeInline(in);
        assertEquals(String.format("\"SET\" \"%s\" \"%s\"(unknown, NEW)", s1, s2), command.toString());
    }

    @Test
    public void testDecodeCommand() {
        String s1 = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 20));
        String s2 = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 20));
        ByteBuf in = Unpooled.wrappedBuffer(bytes(String.format(
                "*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                s1.length(), s1, s2.length(), s2)));
        EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandDecoder());
        channel.writeInbound(in);
        channel.finish();
        Command command = channel.readInbound();
        assertEquals(String.format("\"SET\" \"%s\" \"%s\"(unknown, NEW)", s1, s2), command.toString());
    }

    @Test
    public void testReplayingDecodeCommand() {
        String s1 = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 20));
        String s2_1 = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 10));
        String s2_2 = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 10));

        RedisCommandDecoder decoder = new RedisCommandDecoder();
        EmbeddedChannel channel = new EmbeddedChannel(decoder);

        ByteBuf in1 = Unpooled.wrappedBuffer(bytes(String.format("*3\r\n$3\r\nSET\r\n$%d\r\n%s", s1.length(), s1)));
        channel.writeInbound(in1);
        assertEquals(3, decoder.getBulkStrings().length);
        assertEquals(1, decoder.getCurrBulkStringIndex());

        ByteBuf in2_1 = Unpooled.wrappedBuffer(bytes(String.format(
                "\r\n$%d\r\n%s",
                s2_1.length() + s2_2.length(), s2_1)));
        channel.writeInbound(in2_1);
        assertEquals(2, decoder.getCurrBulkStringIndex());

        ByteBuf in2_2 = Unpooled.wrappedBuffer(bytes(String.format("%s\r\n", s2_2)));
        channel.writeInbound(in2_2);
        channel.finish();
        assertEquals(0, decoder.getCurrBulkStringIndex());
        assertNull(decoder.getBulkStrings());

        Command command = channel.readInbound();
        assertEquals(String.format("\"SET\" \"%s\" \"%s%s\"(unknown, NEW)", s1, s2_1, s2_2), command.toString());
    }

    @Test
    public void testDecodeInlineCommand() {
        String s1 = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 20));
        String s2 = RandomStringUtils.randomAlphabetic(RandomUtils.nextInt(1, 20));
        ByteBuf in = Unpooled.wrappedBuffer(bytes(String.format("SET  %s  %s\r\n", s1, s2)));
        EmbeddedChannel channel = new EmbeddedChannel(new RedisCommandDecoder());
        channel.writeInbound(in);
        channel.finish();
        Command command = channel.readInbound();
        assertEquals(String.format("\"SET\" \"%s\" \"%s\"(unknown, NEW)", s1, s2), command.toString());
    }
}