package org.jrp.monitor.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MetricsGroups {

    private static final Logger LOGGER = LogManager.getLogger(MetricsGroups.class);

    private static final Map<String, MetricsGroup> GROUPS = new ConcurrentHashMap<>();

    private static volatile boolean started = false;

    private static final AtomicReference<ScheduledExecutorService> metricsWriterExecutor = new AtomicReference<>();

    public static void addFirstMetricsHolder(String groupName, MetricsHolder<? extends Metrics> holder) {
        MetricsGroup group = getMetricsGroup(groupName);
        group.addFirstMetricsHolder(holder);
    }

    public static void addLastMetricsHolder(String groupName, MetricsHolder<? extends Metrics> holder) {
        MetricsGroup group = getMetricsGroup(groupName);
        group.addLastMetricsHolder(holder);
    }

    public static void setMetricsLogger(String groupName, Logger logger) {
        MetricsGroup group = getMetricsGroup(groupName);
        group.setOutputLogger(logger);
    }

    public static synchronized void start() {
        if (started) {
            return;
        }

        MetricsWriter metricsWriter = new MetricsWriter();
        BasicThreadFactory threadFactory = new BasicThreadFactory.Builder()
                .namingPattern("MetricsWriterWorker")
                .build();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        executor.scheduleAtFixedRate(() -> metricsWriter.write(GROUPS), 1, 1, TimeUnit.SECONDS);
        LOGGER.info("MetricsGroups writer {} started, running at 1 second fixed rate.", metricsWriter);

        metricsWriterExecutor.set(executor);
        started = true;
    }

    @VisibleForTesting
    static boolean isStarted() {
        return started;
    }

    @VisibleForTesting
    static MetricsGroup getMetricsGroup(String name) {
        return GROUPS.computeIfAbsent(name, k -> new MetricsGroup());
    }

    @VisibleForTesting
    static Map<String, MetricsGroup> getMetricsGroups() {
        return GROUPS;
    }

    @VisibleForTesting
    static ScheduledExecutorService getMetricsWriterExecutor() {
        return metricsWriterExecutor.get();
    }
}
