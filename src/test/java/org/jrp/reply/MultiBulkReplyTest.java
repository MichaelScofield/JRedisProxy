package org.jrp.reply;

import io.lettuce.core.KeyValue;
import io.lettuce.core.ScoredValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.jrp.utils.BytesUtils.bytes;
import static org.jrp.utils.BytesUtils.string;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MultiBulkReplyTest {

    @Test
    public void testCreateMultiBulkReplyFromIntegers() {
        MultiBulkReply reply1 = MultiBulkReply.fromIntegers(new ArrayList<>());
        assertEquals("", reply1.toString());
        MultiBulkReply reply2 = MultiBulkReply.fromIntegers(Arrays.asList(1L, 2L, 3L));
        ByteBuf buffer = Unpooled.buffer();
        reply2.write(buffer);
        assertEquals("*3\r\n:1\r\n:2\r\n:3\r\n", string(ByteBufUtil.getBytes(buffer)));
    }

    @Test
    public void testCreateMultiBulkReplyFromBytesMap() {
        Map<byte[], byte[]> map = new LinkedHashMap<>();
        MultiBulkReply reply1 = MultiBulkReply.fromBytesMap(map);
        assertEquals("", reply1.toString());
        ByteBuf buffer1 = Unpooled.buffer();
        reply1.write(buffer1);
        assertEquals("*0\r\n", string(ByteBufUtil.getBytes(buffer1)));

        map.put(bytes("k1"), bytes("v1"));
        map.put(bytes("k2"), bytes("v2"));
        MultiBulkReply reply2 = MultiBulkReply.fromBytesMap(map);
        assertEquals("\"k1\" \"v1\" \"k2\" \"v2\"", reply2.toString());
        ByteBuf buffer2 = Unpooled.buffer();
        reply2.write(buffer2);
        assertEquals(
                "*4\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n",
                string(ByteBufUtil.getBytes(buffer2)));
    }

    @Test
    public void testCreateMultiBulkReplyFromStringMap() {
        Map<String, String> map = new LinkedHashMap<>();
        MultiBulkReply reply1 = MultiBulkReply.fromStringMap(map);
        assertEquals("", reply1.toString());
        ByteBuf buffer1 = Unpooled.buffer();
        reply1.write(buffer1);
        assertEquals("*0\r\n", string(ByteBufUtil.getBytes(buffer1)));

        map.put("k1", "v1");
        map.put("k2", "v2");
        MultiBulkReply reply2 = MultiBulkReply.fromStringMap(map);
        assertEquals("\"k1\" \"v1\" \"k2\" \"v2\"", reply2.toString());
        ByteBuf buffer2 = Unpooled.buffer();
        reply2.write(buffer2);
        assertEquals(
                "*4\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n",
                string(ByteBufUtil.getBytes(buffer2)));
    }

    @Test
    public void testCreateMultiBulkReplyFromScoreValues() {
        List<ScoredValue<byte[]>> scoredValues = new ArrayList<>();
        MultiBulkReply reply1 = MultiBulkReply.fromScoreValues(scoredValues);
        assertEquals("", reply1.toString());
        ByteBuf buffer1 = Unpooled.buffer();
        reply1.write(buffer1);
        assertEquals("*0\r\n", string(ByteBufUtil.getBytes(buffer1)));

        scoredValues.add(ScoredValue.just(1.0, bytes("v1")));
        scoredValues.add(ScoredValue.just(2.0, bytes("v2")));
        MultiBulkReply reply2 = MultiBulkReply.fromScoreValues(scoredValues);
        assertEquals("\"v1\" \"1\" \"v2\" \"2\"", reply2.toString());
        ByteBuf buffer2 = Unpooled.buffer();
        reply2.write(buffer2);
        assertEquals(
                "*4\r\n$2\r\nv1\r\n$1\r\n1\r\n$2\r\nv2\r\n$1\r\n2\r\n",
                string(ByteBufUtil.getBytes(buffer2)));
    }

    @Test
    public void testCreateMultiBulkReplyFromKeyValues() {
        List<KeyValue<byte[], byte[]>> keyValues = new ArrayList<>();
        MultiBulkReply reply1 = MultiBulkReply.fromKeyValues(keyValues);
        assertEquals("", reply1.toString());
        ByteBuf buffer1 = Unpooled.buffer();
        reply1.write(buffer1);
        assertEquals("*0\r\n", string(ByteBufUtil.getBytes(buffer1)));

        keyValues.add(KeyValue.just(bytes("k1"), bytes("v1")));
        keyValues.add(KeyValue.just(bytes("k2"), bytes("v2")));
        MultiBulkReply reply2 = MultiBulkReply.fromKeyValues(keyValues);
        assertEquals("\"v1\" \"v2\"", reply2.toString());
        ByteBuf buffer2 = Unpooled.buffer();
        reply2.write(buffer2);
        assertEquals("*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n", string(ByteBufUtil.getBytes(buffer2)));
    }

    @Test
    public void testCreateMultiBulkReplyFromCollection() {
        List<String> strings = new ArrayList<>();
        MultiBulkReply reply1 = MultiBulkReply.multiBulkReply(strings);
        assertEquals("", reply1.toString());
        ByteBuf buffer1 = Unpooled.buffer();
        reply1.write(buffer1);
        assertEquals("*0\r\n", string(ByteBufUtil.getBytes(buffer1)));

        strings.add("v1");
        strings.add("v2");
        MultiBulkReply reply2 = MultiBulkReply.multiBulkReply(strings);
        assertEquals("\"v1\" \"v2\"", reply2.toString());
        ByteBuf buffer2 = Unpooled.buffer();
        reply2.write(buffer2);
        assertEquals("*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n", string(ByteBufUtil.getBytes(buffer2)));
    }

    @Test
    public void testCreateRawMultiBulkReply() {
        List<byte[]> strings = new ArrayList<>();
        MultiBulkReply reply1 = MultiBulkReply.rawMultiBulkReply(strings);
        assertEquals("", reply1.toString());
        ByteBuf buffer1 = Unpooled.buffer();
        reply1.write(buffer1);
        assertEquals("*0\r\n", string(ByteBufUtil.getBytes(buffer1)));

        strings.add(bytes("v1"));
        strings.add(bytes("v2"));
        MultiBulkReply reply2 = MultiBulkReply.rawMultiBulkReply(strings);
        assertEquals("\"v1\" \"v2\"", reply2.toString());
        ByteBuf buffer2 = Unpooled.buffer();
        reply2.write(buffer2);
        assertEquals("*2\r\n$2\r\nv1\r\n$2\r\nv2\r\n", string(ByteBufUtil.getBytes(buffer2)));
    }

    @Test
    public void testWriteNilMultiBulkReply() {
        MultiBulkReply reply = new MultiBulkReply(null);
        ByteBuf buffer = Unpooled.buffer();
        reply.write(buffer);
        assertEquals("*-1\r\n", string(ByteBufUtil.getBytes(buffer)));
    }

    @Test
    public void testMultiBulkReplyToString() {
        MultiBulkReply reply1 = new MultiBulkReply(null);
        assertEquals("null", reply1.toString());

        List<String> l1 = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            l1.add("v" + i);
        }
        MultiBulkReply reply2 = MultiBulkReply.multiBulkReply(l1);
        assertEquals(
                "\"v0\" \"v1\" \"v2\" \"v3\" \"v4\" \"v5\" \"v6\" \"v7\" \"v8\" \"v9\"",
                reply2.toString());

        List<String> l2 = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            l2.add("v" + i);
        }
        MultiBulkReply reply3 = MultiBulkReply.multiBulkReply(l2);
        assertEquals(
                "\"v0\" \"v1\" \"v2\" \"v3\" \"v4\" \"v5\" \"v6\" \"v7\" \"v8\" \"v9\" ...(and 90 more)",
                reply3.toString());
    }
}
