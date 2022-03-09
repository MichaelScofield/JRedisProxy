package org.jrp.client.lettuce;

import io.lettuce.core.metrics.CommandLatencyCollectorOptions;
import io.lettuce.core.metrics.DefaultCommandLatencyCollector;
import io.lettuce.core.protocol.ProtocolKeyword;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

class RedisCommandLatencyCollector extends DefaultCommandLatencyCollector {

    private static final Logger LOGGER = LogManager.getLogger(RedisCommandLatencyCollector.class);

    RedisCommandLatencyCollector(CommandLatencyCollectorOptions options) {
        super(options);
    }

    @Override
    public void recordCommandLatency(SocketAddress local, SocketAddress remote, ProtocolKeyword commandType,
                                     long firstResponseLatency, long completionLatency) {
        super.recordCommandLatency(local, remote, commandType, firstResponseLatency, completionLatency);
        String hostport = getHostport(remote);
        int latencyMillis = (int) TimeUnit.NANOSECONDS.toMillis(completionLatency);
        LOGGER.debug("{} {} {}ms", commandType.name(), hostport, latencyMillis);
    }

    private String getHostport(SocketAddress remote) {
        InetSocketAddress remoteAddress = (InetSocketAddress) remote;
        String host = remoteAddress.getAddress().getHostAddress();
        int port = remoteAddress.getPort();
        return host + ":" + port;
    }
}
