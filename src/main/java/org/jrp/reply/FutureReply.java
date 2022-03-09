package org.jrp.reply;

import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

public class FutureReply<T> implements Reply {

    private final CompletionStage<Reply> stage;
    private volatile Reply reply;

    public FutureReply(RedisFuture<T> future, Function<T, Reply> converter) {
        stage = future.handle((t, ex) -> {
            Reply reply;
            if (ex == null) {
                reply = converter.apply(t);
            } else {
                if (ex instanceof RedisCommandExecutionException e) {
                    reply = new ErrorReply(e.getMessage());
                } else {
                    reply = new ErrorReply(ExceptionUtils.getRootCauseMessage(ex));
                }
            }
            return reply;
        });
    }

    public void onComplete(Consumer<Reply> consumer) {
        stage.thenAccept(reply -> {
            FutureReply.this.reply = reply;
            consumer.accept(reply);
        });
    }

    @Override
    public void write(ByteBuf out) {
        reply.write(out);
    }

    @VisibleForTesting
    CompletionStage<Reply> getStage() {
        return stage;
    }

    @VisibleForTesting
    Reply getReply() {
        return reply;
    }
}
