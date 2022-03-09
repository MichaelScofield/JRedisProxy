package org.jrp.server;

import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import org.jrp.cmd.Command;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class RedisServerContextTest {

    @Test
    public void testLocalInThread() throws InterruptedException {
        Command command = new Command();
        EmbeddedChannel channel = new EmbeddedChannel();
        RedisServerContext.fill(command, channel);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Command> expectNullCommand = new AtomicReference<>(new Command());
        AtomicReference<Channel> expectNullChannel = new AtomicReference<>(new EmbeddedChannel());
        new Thread(() -> {
            expectNullCommand.set(RedisServerContext.getCommand());
            expectNullChannel.set(RedisServerContext.getChannel());
            latch.countDown();
        }).start();
        latch.await();

        assertNull(expectNullCommand.get());
        assertNull(expectNullChannel.get());

        assertSame(command, RedisServerContext.getCommand());
        assertSame(channel, RedisServerContext.getChannel());

        RedisServerContext.clear();
        assertNull(RedisServerContext.getCommand());
        assertNull(RedisServerContext.getChannel());
    }
}
