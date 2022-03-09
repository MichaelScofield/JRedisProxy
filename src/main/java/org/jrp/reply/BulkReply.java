package org.jrp.reply;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

import static org.jrp.utils.BytesUtils.bytes;

public class BulkReply implements Reply {

    public static final BulkReply NIL_REPLY = new BulkReply();

    private static final char MARKER = '$';

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.########");

    private final ByteBuf bytes;

    final int capacity;

    private BulkReply() {
        bytes = null;
        capacity = -1;
    }

    public BulkReply(byte[] bytes) {
        this.bytes = Unpooled.wrappedBuffer(bytes);
        capacity = bytes.length;
    }

    public static BulkReply bulkReply(String reply) {
        return reply == null ? NIL_REPLY : new BulkReply(bytes(reply));
    }

    public static BulkReply bulkReply(Double d) {
        return d == null ? NIL_REPLY : new BulkReply(bytes(DECIMAL_FORMAT.format(d.doubleValue())));
    }

    public static BulkReply bulkReply(byte[] reply) {
        return reply == null ? NIL_REPLY : new BulkReply(reply);
    }

    @Override
    public void write(ByteBuf out) {
        out.writeByte(MARKER);
        out.writeBytes(bytes(String.valueOf(capacity)));
        out.writeBytes(CRLF);
        if (capacity > 0) {
            out.writeBytes(bytes);
            out.writeBytes(CRLF);
        } else if (capacity == 0) {
            out.writeBytes(CRLF);
        }
    }

    @Override
    public String toString() {
        return bytes == null ? "null" : (bytes.hasArray()
                ? new String(bytes.array(), bytes.arrayOffset(), capacity, StandardCharsets.UTF_8)
                : bytes.toString(StandardCharsets.UTF_8));
    }
}
