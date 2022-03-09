package org.jrp.monitor;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.reply.BulkReply;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("ClassCanBeRecord")
public class Monitor {

    private static final Logger LOGGER = LogManager.getLogger(Monitor.class);

    private static final AtomicInteger monitorsCount = new AtomicInteger(0);

    private static final Map<ChannelId, Monitor> MONITORS = new ConcurrentHashMap<>();

    private final Channel channel;

    public static void startMonitor(Channel channel) {
        ChannelId channelId = channel.id();
        if (MONITORS.containsKey(channelId)) {
            return;
        }

        Monitor monitor = new Monitor(channel);
        channel.closeFuture().addListener(future -> stopMonitor(channel));

        MONITORS.put(channelId, monitor);
        monitorsCount.getAndIncrement();

        LOGGER.info("Start monitor " + monitor);
    }

    public static void stopMonitor(Channel channel) {
        Monitor monitor = MONITORS.remove(channel.id());
        if (monitor != null) {
            monitorsCount.getAndDecrement();
        }
    }

    public static boolean hasMonitor() {
        return monitorsCount.get() > 0;
    }

    public static void record(int db, String remoteAddr, String cmd) {
        Instant now = Instant.now();
        long epochSecond = now.getEpochSecond();
        long micros = now.getNano() / 1000 % 1_000_000;
        for (Monitor monitor : MONITORS.values()) {
            if (!monitor.channel.isWritable()) {
                continue;
            }
            String record = String.format("%s.%s [%s %s] %s", epochSecond, micros, db, remoteAddr, cmd);
            monitor.channel.writeAndFlush(BulkReply.bulkReply(record));
        }
    }

    Monitor(Channel channel) {
        this.channel = channel;
    }

    @Override
    public String toString() {
        return "Monitor{" +
                "channel=" + channel +
                '}';
    }

    @VisibleForTesting
    static AtomicInteger getMonitorsCount() {
        return monitorsCount;
    }

    @VisibleForTesting
    static Map<ChannelId, Monitor> getMonitors() {
        return MONITORS;
    }
}
