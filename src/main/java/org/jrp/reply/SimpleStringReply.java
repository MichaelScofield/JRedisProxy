package org.jrp.reply;

import io.netty.buffer.ByteBuf;
import org.jrp.utils.BytesUtils;

public class SimpleStringReply implements Reply {

    public static final char MARKER = '+';

    public static final SimpleStringReply OK = new SimpleStringReply("OK");
    public static final SimpleStringReply QUEUED = new SimpleStringReply("QUEUED");
    public static final SimpleStringReply QUIT = new SimpleStringReply("OK");
    public static final SimpleStringReply PONG = new SimpleStringReply("PONG");

    private final byte[] status;

    public static SimpleStringReply from(String str) {
        return switch (str) {
            case "OK" -> OK;
            case "QUEUED" -> QUEUED;
            case "PONG" -> PONG;
            default -> new SimpleStringReply(str);
        };
    }

    public SimpleStringReply(String status) {
        this.status = BytesUtils.bytes(status);
    }

    public SimpleStringReply(byte[] status) {
        this.status = status;
    }

    @Override
    public void write(ByteBuf out) {
        out.writeByte(MARKER);
        out.writeBytes(status);
        out.writeBytes(CRLF);
    }

    @Override
    public String toString() {
        return BytesUtils.string(status);
    }
}
