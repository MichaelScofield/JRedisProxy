package org.jrp.monitor.metrics;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class MetricsGroupTest {

    @Test
    public void testGetTitle() {
        Metrics[] metrics = Stream.concat(
                        Stream.generate(MyMetrics1::new).limit(2),
                        Stream.generate(MyMetrics2::new).limit(2))
                .toArray(Metrics[]::new);
        String title = MetricsGroup.getTitle(metrics);
        assertEquals("Title1  Title1  Title2  Title2", title);
    }

    @Test
    public void testGetStat() {
        Metrics[] metrics = Stream.concat(
                        Stream.generate(MyMetrics1::new).limit(2),
                        Stream.generate(MyMetrics2::new).limit(2))
                .toArray(Metrics[]::new);
        String title = MetricsGroup.getStat(metrics);
        assertEquals("Stat1  Stat1  Stat2  Stat2", title);
    }

    @Test
    public void testAddFirstMetricsHolder() {
        MetricsGroup metricsGroup = new MetricsGroup();

        MetricsHolder<MyMetrics1> holder1 = new MetricsHolder<>(MyMetrics1.class);
        metricsGroup.addFirstMetricsHolder(holder1);
        MetricsHolder<MyMetrics2> holder2 = new MetricsHolder<>(MyMetrics2.class);
        metricsGroup.addFirstMetricsHolder(holder2);

        LinkedList<MetricsHolder<? extends Metrics>> holders = metricsGroup.getHolders();
        assertEquals(2, holders.size());
        assertSame(holder2, holders.get(0));
        assertSame(holder1, holders.get(1));
    }

    @Test
    public void testAddLastMetricsHolder() {
        MetricsGroup metricsGroup = new MetricsGroup();

        MetricsHolder<MyMetrics1> holder1 = new MetricsHolder<>(MyMetrics1.class);
        metricsGroup.addLastMetricsHolder(holder1);
        MetricsHolder<MyMetrics2> holder2 = new MetricsHolder<>(MyMetrics2.class);
        metricsGroup.addLastMetricsHolder(holder2);

        LinkedList<MetricsHolder<? extends Metrics>> holders = metricsGroup.getHolders();
        assertEquals(2, holders.size());
        assertSame(holder1, holders.get(0));
        assertSame(holder2, holders.get(1));
    }

    @Test
    public void testRenewMetrics() {
        MetricsGroup metricsGroup = new MetricsGroup();

        MetricsHolder<MyMetrics1> holder1 = new MetricsHolder<>(MyMetrics1.class);
        MetricsHolder<MyMetrics2> holder2 = new MetricsHolder<>(MyMetrics2.class);

        LinkedList<MetricsHolder<? extends Metrics>> holders = metricsGroup.getHolders();
        holders.add(holder1);
        holders.add(holder2);

        Metrics[] metrics1 = metricsGroup.renewMetrics();
        assertEquals(2, metrics1.length);
        assertTrue(metrics1[0] instanceof MyMetrics1);
        assertTrue(metrics1[1] instanceof MyMetrics2);

        Metrics[] metrics2 = metricsGroup.renewMetrics();
        assertEquals(2, metrics2.length);
        assertTrue(metrics2[0] instanceof MyMetrics1);
        assertTrue(metrics2[1] instanceof MyMetrics2);

        assertNotSame(metrics1[0], metrics2[0]);
        assertNotSame(metrics1[1], metrics2[1]);
    }
}
