package org.jrp.monitor;

import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.jrp.reply.BulkReply;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class MonitorTest {

    @AfterEach
    public void cleanup() {
        Monitor.getMonitorsCount().set(0);
        Monitor.getMonitors().clear();
    }

    @Test
    public void testStartMonitor() throws InterruptedException, ExecutionException {
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());

        // Start monitoring on the same channel multiple times,
        // and only one monitor will be created.
        Monitor.startMonitor(channel);
        Monitor.startMonitor(channel);
        Monitor.startMonitor(channel);

        assertEquals(1, Monitor.getMonitorsCount().get());
        assertEquals(1, Monitor.getMonitors().size());

        // Monitor will be automatically stopped when channel is closed.
        channel.close().sync().get();

        assertEquals(0, Monitor.getMonitorsCount().get());
        assertEquals(0, Monitor.getMonitors().size());
    }

    @Test
    public void testStopMonitor() {
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());
        Monitor.getMonitors().put(channel.id(), new Monitor(channel));
        Monitor.getMonitorsCount().getAndIncrement();

        Monitor.stopMonitor(channel);

        assertEquals(0, Monitor.getMonitorsCount().get());
        assertEquals(0, Monitor.getMonitors().size());
    }

    @Test
    public void testHasMonitor() {
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance());
        Monitor.startMonitor(channel);
        assertTrue(Monitor.hasMonitor());

        Monitor.stopMonitor(channel);
        assertFalse(Monitor.hasMonitor());
    }

    @Test
    public void testRecord() {
        EmbeddedChannel channel1 = new EmbeddedChannel(DefaultChannelId.newInstance());
        Monitor.startMonitor(channel1);
        EmbeddedChannel channel2 = new EmbeddedChannel(DefaultChannelId.newInstance());
        Monitor.startMonitor(channel2);

        Monitor.record(0, "1.2.3.4:1234", "GET foo");

        String format = "\\d+\\.\\d+ \\[0 1\\.2\\.3\\.4:1234] GET foo";
        BulkReply reply1 = channel1.readOutbound();
        assertTrue(reply1.toString().matches(format));
        BulkReply reply2 = channel2.readOutbound();
        assertTrue(reply2.toString().matches(format));
    }
}
