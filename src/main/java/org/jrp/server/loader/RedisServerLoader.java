package org.jrp.server.loader;

import org.jrp.config.ProxyConfig;
import org.jrp.server.RedisServer;

public abstract class RedisServerLoader {

    public abstract RedisServer load(ProxyConfig proxyConfig);
}
