package org.jrp.client.lettuce;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LettuceRedisClientBuilderTest {

    @Test
    public void testBuild() {
        LettuceRedisClientBuilder builder = new LettuceRedisClientBuilder()
                .withHost("localhost")
                .withPort(6379)
                .withPassword("hello".toCharArray())
                .withTimeoutMillis(1000)
                .withRequestQueueSize(2022);
        LettuceRedisClient client = builder.build();

        LettuceRedisClientBuilder clientBuilder = client.getBuilder();
        assertEquals(builder.getHost(), clientBuilder.getHost());
        assertEquals(builder.getPort(), clientBuilder.getPort());
        assertEquals(builder.getPassword(), clientBuilder.getPassword());
        assertEquals(builder.getTimeoutMillis(), clientBuilder.getTimeoutMillis());
        assertEquals(builder.getRequestQueueSize(), clientBuilder.getRequestQueueSize());
    }

    @Test
    public void testBuilderWithDefaultValues() {
        LettuceRedisClientBuilder builder = new LettuceRedisClientBuilder();

        assertEquals(LettuceRedisClientBuilder.DEFAULT_HOST, builder.getHost());
        assertEquals(LettuceRedisClientBuilder.DEFAULT_PORT, builder.getPort());
        assertEquals(LettuceRedisClientBuilder.DEFAULT_TIMEOUT_MILLIS, builder.getTimeoutMillis());
        assertEquals(LettuceRedisClientBuilder.DEFAULT_REQUEST_QUEUE_SIZE, builder.getRequestQueueSize());
        assertNull(builder.getPassword());
    }
}
