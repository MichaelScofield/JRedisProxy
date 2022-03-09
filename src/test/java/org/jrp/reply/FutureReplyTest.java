package org.jrp.reply;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandType;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.RandomStringUtils;
import org.jrp.utils.BytesUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class FutureReplyTest {

    record MyReply(String content) implements Reply {

        @Override
        public void write(ByteBuf out) {
            out.writeBytes(BytesUtils.bytes(content));
        }

        @Override
        public String toString() {
            return "MyReply{" +
                    "content='" + content + '\'' +
                    '}';
        }
    }

    @Test
    public void testFutureReplyCompleteNormally() throws ExecutionException, InterruptedException {
        AsyncCommand<String, String, String> asyncCommand = createAsyncCommand();
        FutureReply<String> futureReply = new FutureReply<>(asyncCommand, MyReply::new);

        String s = RandomStringUtils.randomAlphabetic(10);
        assertTrue(asyncCommand.complete(s));

        Reply reply = futureReply.getStage().toCompletableFuture().get();
        assertTrue(reply instanceof MyReply);
        assertEquals(s, ((MyReply) reply).content);
    }

    @Test
    public void testFutureReplyCompleteExceptionally() throws ExecutionException, InterruptedException {
        AsyncCommand<String, String, String> asyncCommand = createAsyncCommand();
        FutureReply<String> futureReply = new FutureReply<>(asyncCommand, MyReply::new);

        String s = RandomStringUtils.randomAlphabetic(10);
        assertTrue(asyncCommand.completeExceptionally(new Exception("not root cause", new Exception(s))));

        Reply reply = futureReply.getStage().toCompletableFuture().get();
        assertTrue(reply instanceof ErrorReply);
        assertEquals("Exception: " + s, reply.toString());
    }

    @Test
    public void testFutureReplyFailedToConvert() {
        AsyncCommand<String, String, String> asyncCommand = createAsyncCommand();
        FutureReply<String> futureReply = new FutureReply<>(asyncCommand, s -> {
            throw new RuntimeException(s);
        });

        String s = RandomStringUtils.randomAlphabetic(10);
        assertTrue(asyncCommand.complete(s));

        ExecutionException e = assertThrows(ExecutionException.class,
                () -> futureReply.getStage().toCompletableFuture().get());
        assertEquals("java.lang.RuntimeException: " + s, e.getMessage());
    }

    @Test
    public void testOnComplete() throws ExecutionException, InterruptedException {
        AsyncCommand<String, String, String> asyncCommand = createAsyncCommand();
        FutureReply<String> futureReply = new FutureReply<>(asyncCommand, MyReply::new);
        assertNull(futureReply.getReply());

        AtomicReference<String> completedReply = new AtomicReference<>(null);
        futureReply.onComplete(reply -> completedReply.set(reply.toString()));

        String s = RandomStringUtils.randomAlphabetic(10);
        assertTrue(asyncCommand.complete(s));

        futureReply.getStage().toCompletableFuture().get();
        assertEquals("MyReply{content='" + s + "'}", completedReply.get());

        Reply reply = futureReply.getReply();
        assertTrue(reply instanceof MyReply);
        assertEquals(s, ((MyReply) reply).content());
    }

    AsyncCommand<String, String, String> createAsyncCommand() {
        return new AsyncCommand<>(new Command<>(CommandType.GET, new ValueOutput<>(new StringCodec())));
    }
}
