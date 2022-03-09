package org.jrp.monitor.metrics;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetricsWriterTest {

    @Test
    public void testWrite() throws InterruptedException {
        Map<String, MetricsGroup> metricsGroups = new HashMap<>();

        MetricsHolder<MyMetrics1> group1Holder1 = new MetricsHolder<>(MyMetrics1.class);
        MetricsHolder<MyMetrics1> group1Holder2 = new MetricsHolder<>(MyMetrics1.class);
        MetricsGroup group1 = new MetricsGroup();
        group1.addLastMetricsHolder(group1Holder1);
        group1.addLastMetricsHolder(group1Holder2);

        List<String> logs1 = new ArrayList<>();
        group1.setOutputLogger(createLogger(logs1));

        metricsGroups.put("myGroup1", group1);

        MetricsHolder<MyMetrics2> group2Holder1 = new MetricsHolder<>(MyMetrics2.class);
        MetricsHolder<MyMetrics2> group2Holder2 = new MetricsHolder<>(MyMetrics2.class);
        MetricsGroup group2 = new MetricsGroup();
        group2.addLastMetricsHolder(group2Holder1);
        group2.addLastMetricsHolder(group2Holder2);

        List<String> logs2 = new ArrayList<>();
        group2.setOutputLogger(createLogger(logs2));

        metricsGroups.put("myGroup2", group2);

        MetricsWriter writer = new MetricsWriter();
        for (int i = 0; i < 10; i++) {
            writer.write(metricsGroups);
            TimeUnit.SECONDS.sleep(1);
        }

        assertEquals(12, logs1.size());
        assertEquals(1, logs1.stream()
                .filter(log -> log.equals("myGroup1"))
                .count());
        assertEquals(1, logs1.stream()
                .filter(log -> log.equals("TIME                 Title1  Title1"))
                .count());
        assertEquals(10, logs1.stream()
                .filter(log -> log.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} {2}Stat1 {2}Stat1"))
                .count());

        assertEquals(12, logs2.size());
        assertEquals(1, logs2.stream()
                .filter(log -> log.equals("myGroup2"))
                .count());
        assertEquals(1, logs2.stream()
                .filter(log -> log.equals("TIME                 Title2  Title2"))
                .count());
        assertEquals(10, logs2.stream()
                .filter(log -> log.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} {2}Stat2 {2}Stat2"))
                .count());
    }

    private Logger createLogger(List<String> logs) {
        return (Logger) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                new Class[]{Logger.class}, (proxy, method, args) -> {
                    // intercept method "void info(String message);"
                    if (method.getName().equals("info") && args.length == 1 && args[0] instanceof String s) {
                        logs.add(s);
                    }
                    //noinspection SuspiciousInvocationHandlerImplementation
                    return null;
                });
    }
}
