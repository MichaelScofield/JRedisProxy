package org.jrp.reply;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.jrp.utils.BytesUtils.bytes;
import static org.jrp.utils.BytesUtils.string;
import static org.junit.jupiter.api.Assertions.*;

public class BulkReplyTest {

    @Test
    public void testCreateBulkReply() {
        String s = "123456789";
        BulkReply reply = new BulkReply(bytes(s));
        assertEquals(s.length(), reply.capacity);
        assertEquals(s, reply.toString());

        ByteBuf buffer = Unpooled.buffer();
        reply.write(buffer);
        assertArrayEquals(bytes("$9\r\n" + s + "\r\n"), ByteBufUtil.getBytes(buffer));
    }

    @Test
    public void testCreateBulkRepliesStatically() {
        BulkReply reply1 = BulkReply.bulkReply((String) null);
        assertSame(BulkReply.NIL_REPLY, reply1);

        String s1 = RandomStringUtils.randomAlphabetic(10);
        BulkReply reply2 = BulkReply.bulkReply(s1);
        assertEquals(s1, reply2.toString());

        BulkReply reply3 = BulkReply.bulkReply((Double) null);
        assertSame(BulkReply.NIL_REPLY, reply3);

        Double d1 = 1234.1234567812345678;
        BulkReply reply4 = BulkReply.bulkReply(d1);
        assertEquals("1234.12345678", reply4.toString());

        Double d2 = 1.234;
        BulkReply reply5 = BulkReply.bulkReply(d2);
        assertEquals("1.234", reply5.toString());

        BulkReply reply6 = BulkReply.bulkReply((byte[]) null);
        assertSame(BulkReply.NIL_REPLY, reply6);

        String s2 = RandomStringUtils.randomAscii(10);
        BulkReply reply7 = BulkReply.bulkReply(bytes(s2));
        assertEquals(s2, reply7.toString());
    }

    @Test
    public void testWrite() {
        String randomString = RandomStringUtils.randomAlphabetic(10);

        List<BulkReply> replies = new ArrayList<>();
        replies.add(BulkReply.NIL_REPLY);
        replies.add(new BulkReply(new byte[0]));
        replies.add(new BulkReply(bytes(randomString)));

        List<String> expects = new ArrayList<>();
        expects.add("$-1\r\n");
        expects.add("$0\r\n\r\n");
        expects.add("$" + randomString.length() + "\r\n" + randomString + "\r\n");

        for (int i = 0; i < replies.size(); i++) {
            BulkReply reply = replies.get(i);
            ByteBuf buffer = Unpooled.buffer();
            reply.write(buffer);
            assertEquals(expects.get(i), string(ByteBufUtil.getBytes(buffer)));
        }
    }
}
