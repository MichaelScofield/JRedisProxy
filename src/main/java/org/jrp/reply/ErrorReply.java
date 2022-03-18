package org.jrp.reply;

import io.netty.buffer.ByteBuf;
import org.jrp.utils.BytesUtils;

public record ErrorReply(String error) implements Reply {

    public static final ErrorReply NOT_IMPL = new ErrorReply("Not Implemented");
    public static final ErrorReply NIL_REPLY = new ErrorReply("nil");
    public static final ErrorReply SYNTAX_ERROR = new ErrorReply("Syntax Error");
    public static final ErrorReply BUSY_ERROR = new ErrorReply("Busy");
    public static final ErrorReply READONLY_ERROR = new ErrorReply("Readonly");

    private static final char MARKER = '-';

    @Override
    public void write(ByteBuf out) {
        out.writeByte(MARKER);
        out.writeBytes(BytesUtils.bytes(error));
        out.writeBytes(CRLF);
    }

    @Override
    public String toString() {
        return error;
    }
}
