package org.jrp.reply;

import io.netty.buffer.ByteBuf;

import java.util.function.Supplier;

public class AsyncReply<T extends Reply> implements Reply {

    private final Supplier<T> replySupplier;

    private volatile T reply;

    public AsyncReply(Supplier<T> replySupplier) {
        this.replySupplier = replySupplier;
    }

    public T getReply() {
        if (reply == null) {
            synchronized (this) {
                if (reply == null) {
                    reply = replySupplier.get();
                }
            }
        }
        return reply;
    }

    @Override
    public void write(ByteBuf out) {
        getReply().write(out);
    }
}
