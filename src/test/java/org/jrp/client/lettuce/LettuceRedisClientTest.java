package org.jrp.client.lettuce;

import io.lettuce.core.api.async.RedisAsyncCommands;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class LettuceRedisClientTest {

    @Test
    public void testGetClient() throws ExecutionException, InterruptedException, TimeoutException {
        LettuceRedisClientBuilder builder = new LettuceRedisClientBuilder();
        LettuceRedisClient lettuceRedisClient = builder.build();

        RedisAsyncCommands<byte[], byte[]> client = lettuceRedisClient.getClient();
        String resp = client.ping().get(1, TimeUnit.SECONDS);
        assertEquals("PONG", resp);
    }

    @Test
    public void testGetClientConcurrently() throws InterruptedException {
        LettuceRedisClientBuilder builder = new LettuceRedisClientBuilder();
        LettuceRedisClient lettuceRedisClient = builder.build();

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(2);

        List<RedisAsyncCommands<byte[], byte[]>> thread1GotClients = new ArrayList<>();
        List<RedisAsyncCommands<byte[], byte[]>> thread2GotClients = new ArrayList<>();

        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException ignored) {
                return;
            }

            for (int i = 0; i < 100; i++) {
                thread1GotClients.add(lettuceRedisClient.getClient(1));
            }

            stopLatch.countDown();
        }).start();
        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException ignored) {
                return;
            }

            for (int i = 0; i < 100; i++) {
                thread2GotClients.add(lettuceRedisClient.getClient(1));
            }

            stopLatch.countDown();
        }).start();
        startLatch.countDown();
        stopLatch.await();

        ConcurrentMap<Integer, RedisAsyncCommands<byte[], byte[]>> dbClients = lettuceRedisClient.getDbClients();
        RedisAsyncCommands<byte[], byte[]> expectedClient = dbClients.get(1);
        assertNotNull(expectedClient);

        for (RedisAsyncCommands<byte[], byte[]> thread1GotClient : thread1GotClients) {
            assertSame(expectedClient, thread1GotClient);
        }
        for (RedisAsyncCommands<byte[], byte[]> thread2GotClient : thread2GotClients) {
            assertSame(expectedClient, thread2GotClient);
        }
    }
}
