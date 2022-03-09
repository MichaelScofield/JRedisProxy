package org.jrp.reply;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.jrp.utils.BytesUtils.bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class AsyncReplyTest {

    static class MyReply implements Reply {

        final String randomString = RandomStringUtils.randomAscii(10);

        @Override
        public void write(ByteBuf out) {
            out.writeBytes(bytes(randomString));
        }
    }

    @Test
    public void testGetReplyConcurrently() throws InterruptedException {
        AsyncReply<Reply> reply = new AsyncReply<>(MyReply::new);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(2);

        List<Reply> thread1GotObjects = new ArrayList<>(100);
        List<Reply> thread2GotObjects = new ArrayList<>(100);

        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                return;
            }
            for (int i = 0; i < 100; i++) {
                thread1GotObjects.add(reply.getReply());
            }
            stopLatch.countDown();
        }).start();
        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                return;
            }
            for (int i = 0; i < 100; i++) {
                thread2GotObjects.add(reply.getReply());
            }
            stopLatch.countDown();
        }).start();

        startLatch.countDown();
        stopLatch.await();

        Reply expected = thread1GotObjects.get(0);
        for (int i = 0; i < 100; i++) {
            assertSame(expected, thread1GotObjects.get(i));
            assertSame(expected, thread2GotObjects.get(i));
        }
    }

    @Test
    public void testWrite() {
        AsyncReply<Reply> reply = new AsyncReply<>(MyReply::new);
        ByteBuf buffer = Unpooled.buffer();
        reply.write(buffer);
        assertArrayEquals(
                bytes(((MyReply) reply.getReply()).randomString),
                ByteBufUtil.getBytes(buffer));
    }
}
