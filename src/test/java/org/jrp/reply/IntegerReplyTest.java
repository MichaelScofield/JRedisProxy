package org.jrp.reply;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomUtils;
import org.jrp.utils.BytesUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IntegerReplyTest {

    @Test
    public void testIntegerReply() {
        long l1 = RandomUtils.nextLong(0, Long.MAX_VALUE);
        IntegerReply reply1 = new IntegerReply(l1);
        assertEquals(l1, reply1.integer());

        long l2 = RandomUtils.nextLong(0, Long.MAX_VALUE);
        IntegerReply reply2 = IntegerReply.integer(l2);
        assertEquals(l2, reply2.integer());

        IntegerReply reply3 = IntegerReply.integer(false);
        assertEquals(0, reply3.integer());

        IntegerReply reply4 = IntegerReply.integer(true);
        assertEquals(1, reply4.integer());
    }

    @Test
    public void testWrite() {
        long l = RandomUtils.nextLong(0, Long.MAX_VALUE);
        IntegerReply reply = new IntegerReply(l);
        ByteBuf buffer = Unpooled.buffer();
        reply.write(buffer);
        assertEquals(":" + l + "\r\n", BytesUtils.string(ByteBufUtil.getBytes(buffer)));
    }
}
