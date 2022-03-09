package org.jrp.server.loader;

import org.jrp.config.ProxyConfig;
import org.jrp.server.RedisServer;
import org.jrp.server.RedisproxyAsyncServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RedisproxyAsyncServerLoaderTest {

    @Test
    public void testLoad() {
        RedisproxyAsyncServerLoader loader = new RedisproxyAsyncServerLoader();
        RedisServer server = loader.load(new ProxyConfig());
        Assertions.assertTrue(server instanceof RedisproxyAsyncServer);
    }
}
