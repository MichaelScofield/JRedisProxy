package org.jrp.reply;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomStringUtils;
import org.jrp.utils.BytesUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class SimpleStringReplyTest {

    @Test
    public void testFromString() {
        assertSame(SimpleStringReply.OK, SimpleStringReply.from("OK"));
        assertSame(SimpleStringReply.QUEUED, SimpleStringReply.from("QUEUED"));
        assertSame(SimpleStringReply.PONG, SimpleStringReply.from("PONG"));

        String s = RandomStringUtils.randomAlphabetic(10);
        SimpleStringReply reply = SimpleStringReply.from(s);
        assertEquals(s, reply.toString());
    }

    @Test
    public void testCreateSimpleStringReply() {
        String s1 = RandomStringUtils.randomAlphabetic(10);
        SimpleStringReply reply1 = new SimpleStringReply(s1);
        assertEquals(s1, reply1.toString());

        String s2 = RandomStringUtils.randomAlphabetic(10);
        SimpleStringReply reply2 = new SimpleStringReply(BytesUtils.bytes(s2));
        assertEquals(s2, reply2.toString());
    }

    @Test
    public void testWrite() {
        String s = RandomStringUtils.randomAlphabetic(10);
        SimpleStringReply reply = new SimpleStringReply(s);
        ByteBuf buffer = Unpooled.buffer();
        reply.write(buffer);
        assertEquals("+" + s + "\r\n", BytesUtils.string(ByteBufUtil.getBytes(buffer)));
    }
}
