package org.jrp.reply;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomStringUtils;
import org.jrp.utils.BytesUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ErrorReplyTest {

    @Test
    public void testWrite() {
        String s = RandomStringUtils.randomAscii(10);
        ErrorReply reply = new ErrorReply(s);
        ByteBuf buffer = Unpooled.buffer();
        reply.write(buffer);
        assertEquals("-" + s + "\r\n", BytesUtils.string(ByteBufUtil.getBytes(buffer)));
    }
}
