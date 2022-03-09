package org.jrp.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProxyConfigTest {

    @Test
    public void testLoadProxyConfig() throws IOException {
        URL confRes = ProxyConfigTest.class.getResource("/redisproxy-async.yaml");
        String confFile = Objects.requireNonNull(confRes).getPath();
        ProxyConfig proxyConfig = ProxyConfig.loadProxyConfig(confFile);

        assertEquals(16379, proxyConfig.getPort());
        assertEquals("redisproxy_test", proxyConfig.getName());
        assertEquals(20, proxyConfig.getSlowlogLogSlowerThan());
        assertEquals(1, proxyConfig.getHandlerThreads());
        assertEquals(100, proxyConfig.getHugeCommandThreshold());
        assertEquals(200, proxyConfig.getHugeReplyThreshold());
        assertTrue(proxyConfig.isUseNettyEpoll());
        assertEquals(1024, proxyConfig.getBacklog());
        assertEquals(2, proxyConfig.getIoThreads());
        assertTrue(proxyConfig.isReadOnly());
        assertTrue(proxyConfig.isUseIdleStateHandler());
        assertEquals("org.jrp.server.loader.RedisproxyAsyncServerLoader", proxyConfig.getRedisServerLoader());
        assertEquals(1024, proxyConfig.getMaxQueuedCommands());

        Map<String, String> renameCommands = proxyConfig.getRenameCommands();
        assertEquals("whatareyoudoing", renameCommands.get("monitor"));
        assertEquals("", renameCommands.get("keys"));
    }
}
