package org.jrp.monitor.metrics;

import org.jrp.monitor.counter.CounterFactory;
import org.jrp.monitor.counter.LongCounter;

public class RedisproxyStatusMetrics extends MetricsHolder<RedisproxyStatusMetrics> implements Metrics {

    public static final RedisproxyStatusMetrics INSTANCE = new RedisproxyStatusMetrics();

    public final LongCounter threads = CounterFactory.createLongCounter();

    public final LongCounter queued = CounterFactory.createLongCounter();

    public final LongCounter activeConn = CounterFactory.createLongCounter();

    private static final String pattern = "%-7s  %-10s  %-11s";

    private static final String TITLE = String.format(pattern, "THREADS", "CMD_QUEUED", "ACTIVE_CONN");

    private RedisproxyStatusMetrics() {
        super(RedisproxyStatusMetrics.class);
    }

    @Override
    public String getTitle() {
        return TITLE;
    }

    @Override
    public String getStat() {
        return String.format(pattern, threads.get(), queued.get(), activeConn.get());
    }

    @Override
    RedisproxyStatusMetrics newMetricsInstance() {
        return this;
    }
}
