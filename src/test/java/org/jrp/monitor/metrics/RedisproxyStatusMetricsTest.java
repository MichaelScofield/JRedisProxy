package org.jrp.monitor.metrics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class RedisproxyStatusMetricsTest {

    @Test
    public void testIsSingleton() {
        MetricsHolder<RedisproxyStatusMetrics> holder = RedisproxyStatusMetrics.INSTANCE;
        assertSame(RedisproxyStatusMetrics.INSTANCE, holder.getCurrent());
        assertSame(RedisproxyStatusMetrics.INSTANCE, holder.renewMetrics());
        assertSame(RedisproxyStatusMetrics.INSTANCE, holder.renewMetrics());
    }

    @Test
    public void testGetTitle() {
        assertEquals("THREADS  CMD_QUEUED  ACTIVE_CONN", RedisproxyStatusMetrics.INSTANCE.getTitle());
    }
}
