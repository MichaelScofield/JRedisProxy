package org.jrp.reply;

import io.netty.buffer.ByteBuf;

import static org.jrp.utils.BytesUtils.bytes;

public record IntegerReply(long integer) implements Reply {

    public static final char MARKER = ':';

    public static IntegerReply integer(long integer) {
        return new IntegerReply(integer);
    }

    public static IntegerReply integer(boolean b) {
        return b ? integer(1) : integer(0);
    }

    @Override
    public void write(ByteBuf out) {
        out.writeByte(MARKER);
        out.writeBytes(bytes(String.valueOf(integer)));
        out.writeBytes(CRLF);
    }

    public String toString() {
        return String.valueOf(integer);
    }
}
