package org.jrp.client.lettuce;

import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.metrics.CommandLatencyCollectorOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LettuceRedisClient {

    private static final Logger LOGGER = LogManager.getLogger(LettuceRedisClient.class);

    private final LettuceRedisClientBuilder builder;
    private final ConcurrentMap<Integer, RedisAsyncCommands<byte[], byte[]>> dbClients = new ConcurrentHashMap<>();

    LettuceRedisClient(LettuceRedisClientBuilder builder) {
        this.builder = builder;
    }

    /**
     * Get Redis Client for DB '0', lazily.
     */
    public RedisAsyncCommands<byte[], byte[]> getClient() {
        return getClient(0);
    }

    public RedisAsyncCommands<byte[], byte[]> getClient(int db) {
        return dbClients.computeIfAbsent(db, k -> {
            RedisClient redisClient = createRedisClient(k);
            StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);
            return connection.async();
        });
    }

    private RedisClient createRedisClient(int db) {
        RedisURI.Builder uriBuilder = RedisURI.Builder.redis(builder.getHost(), builder.getPort());
        uriBuilder.withDatabase(db);
        if (builder.getPassword() != null) {
            uriBuilder.withPassword(builder.getPassword());
        }
        uriBuilder.withTimeout(Duration.ofMillis(builder.getTimeoutMillis()));
        RedisURI uri = uriBuilder.build();

        CommandLatencyCollectorOptions latencyCollectorOptions = CommandLatencyCollectorOptions.create();
        RedisCommandLatencyCollector latencyCollector = new RedisCommandLatencyCollector(latencyCollectorOptions);

        ClientResources clientResources = DefaultClientResources.builder()
                .commandLatencyRecorder(latencyCollector)
                .build();
        RedisClient client = RedisClient.create(clientResources, uri);

        ClientOptions clientOptions = ClientOptions.builder()
                .requestQueueSize(builder.getRequestQueueSize())
                .timeoutOptions(TimeoutOptions.builder()
                        .fixedTimeout(Duration.ofMillis(builder.getTimeoutMillis()))
                        .build())
                .build();
        client.setOptions(clientOptions);
        LOGGER.info("created RedisClient {}", client);
        return client;
    }

    @VisibleForTesting
    LettuceRedisClientBuilder getBuilder() {
        return builder;
    }

    @VisibleForTesting
    ConcurrentMap<Integer, RedisAsyncCommands<byte[], byte[]>> getDbClients() {
        return dbClients;
    }
}
