package org.jrp.server.loader;

import org.jrp.client.lettuce.LettuceRedisClient;
import org.jrp.client.lettuce.LettuceRedisClientBuilder;
import org.jrp.config.ProxyConfig;
import org.jrp.server.RedisServer;
import org.jrp.server.RedisproxyAsyncServer;

public class RedisproxyAsyncServerLoader extends RedisServerLoader {

    @Override
    public RedisServer load(ProxyConfig proxyConfig) {
        // TODO Set backend Redis host and port in proxy-config.yaml
        LettuceRedisClient dao = new LettuceRedisClientBuilder().build();
        return new RedisproxyAsyncServer(proxyConfig, dao);
    }
}
