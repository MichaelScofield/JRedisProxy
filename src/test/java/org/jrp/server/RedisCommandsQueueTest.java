package org.jrp.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.cmd.Command;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.IllegalCommandException;
import org.jrp.reply.IntegerReply;
import org.jrp.reply.Reply;
import org.jrp.server.handler.RedisCommandHandler;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RedisCommandsQueueTest {

    private static final Logger LOGGER = LogManager.getLogger(RedisCommandsQueueTest.class);

    @Test
    public void testCommandMatchReplyInSequence() throws InterruptedException, IllegalCommandException {
        int channelCount = 5;
        Channel[] channels = new Channel[channelCount];
        Map<ChannelId, Integer> channelIndex = new HashMap<>();
        for (int i = 0; i < channelCount; i++) {
            ChannelId channelId = DefaultChannelId.newInstance();
            channels[i] = new EmbeddedChannel(channelId);
            channelIndex.put(channelId, i);
        }

        AtomicLong[] expectedCommandIds = new AtomicLong[channelCount];
        Arrays.setAll(expectedCommandIds, AtomicLong::new);
        RedisCommandHandler handler = new RedisCommandHandler(null, new ProxyConfig()) {
            @Override
            public void flush(Channel channel, Command command, Reply reply) {
                assertEquals(command.id, ((IntegerReply) reply).integer());
                Integer i = channelIndex.get(channel.id());
                AtomicLong expectedCommandId = expectedCommandIds[i];
                assertEquals(expectedCommandId.getAndAdd(channelCount), command.id);
            }
        };
        RedisCommandsQueue queue = new RedisCommandsQueue(handler);
        int commandHandlersNum = channelCount * 2;
        CountDownLatch latch = new CountDownLatch(channelCount + commandHandlersNum);

        AtomicLong producedCommandsCount = new AtomicLong(channelCount * 100_0000);
        AtomicLong[] awaitCommands = new AtomicLong[channelCount];
        Arrays.setAll(awaitCommands, AtomicLong::new);
        AtomicLong[] pendingCommands = new AtomicLong[channelCount];
        Arrays.setAll(pendingCommands, value -> new AtomicLong(0));
        ExecutorService commandProducers = Executors.newFixedThreadPool(channelCount);
        for (int i = 0; i < channelCount; i++) {
            int j = i;
            commandProducers.execute(() -> {
                // every channel is bound to a specific thread, simulating Netty's guarantee
                Channel channel = channels[j];
                queue.onChannelActive(channel);
                while (!Thread.currentThread().isInterrupted() &&
                        producedCommandsCount.getAndDecrement() >= 0) {
                    queue.add(channel, new Command(awaitCommands[j].getAndAdd(channelCount)));
                    pendingCommands[j].getAndIncrement();
                }
                latch.countDown();
            });
        }

        AtomicLong[] readyCommands = new AtomicLong[channelCount];
        Arrays.setAll(readyCommands, AtomicLong::new);
        AtomicLong consumedCommandsCount = new AtomicLong(channelCount * 100_0000);
        ExecutorService commandHandler = Executors.newFixedThreadPool(commandHandlersNum);
        for (int i = 0; i < commandHandlersNum; i++) {
            commandHandler.execute(() -> {
                while (!Thread.currentThread().isInterrupted() && consumedCommandsCount.get() >= 0) {
                    int r = RandomUtils.nextInt(0, channelCount);
                    if (pendingCommands[r].decrementAndGet() < 0) {
                        pendingCommands[r].getAndIncrement();
                        continue;
                    }
                    consumedCommandsCount.getAndDecrement();
                    Channel channel = channels[r];
                    long commandId = readyCommands[r].getAndAdd(channelCount);
                    queue.set(channel, commandId, new IntegerReply(commandId));
                    queue.flushReadyReplies(channel);
                }
                latch.countDown();
            });
        }

        new Thread(() -> {
            while (latch.getCount() > 0) {
                for (int i = 0; i < channelCount; i++) {
                    LOGGER.debug("""
                                                                    
                                    \tchannel {} max await command {}
                                    \tchannel {} max ready command {}
                                    \tchannel {} expecting command {} reply
                                    """,
                            i, awaitCommands[i].get(),
                            i, readyCommands[i].get(),
                            i, expectedCommandIds[i].get());
                }
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }).start();
        latch.await();
    }
}
