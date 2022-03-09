package org.jrp.monitor.metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.*;

public class MetricsGroupsTest {

    @AfterEach
    public void cleanup() {
        MetricsGroups.getMetricsGroups().clear();
    }

    @Test
    public void testAddFirstMetricsHolder() {
        MetricsHolder<MyMetrics1> group1Holder1 = new MetricsHolder<>(MyMetrics1.class);
        MetricsHolder<MyMetrics2> group1Holder2 = new MetricsHolder<>(MyMetrics2.class);
        MetricsHolder<MyMetrics1> group2Holder1 = new MetricsHolder<>(MyMetrics1.class);
        MetricsHolder<MyMetrics2> group2Holder2 = new MetricsHolder<>(MyMetrics2.class);

        MetricsGroups.addFirstMetricsHolder("myGroup1", group1Holder1);
        MetricsGroups.addFirstMetricsHolder("myGroup1", group1Holder2);
        MetricsGroups.addFirstMetricsHolder("myGroup2", group2Holder1);
        MetricsGroups.addFirstMetricsHolder("myGroup2", group2Holder2);

        MetricsGroup myGroup1 = MetricsGroups.getMetricsGroup("myGroup1");
        LinkedList<MetricsHolder<? extends Metrics>> holders1 = myGroup1.getHolders();
        assertEquals(2, holders1.size());
        assertSame(group1Holder2, holders1.get(0));
        assertSame(group1Holder1, holders1.get(1));

        MetricsGroup myGroup2 = MetricsGroups.getMetricsGroup("myGroup2");
        LinkedList<MetricsHolder<? extends Metrics>> holders2 = myGroup2.getHolders();
        assertEquals(2, holders2.size());
        assertSame(group2Holder2, holders2.get(0));
        assertSame(group2Holder1, holders2.get(1));
    }

    @Test
    public void testAddLastMetricsHolder() {
        MetricsHolder<MyMetrics1> group1Holder1 = new MetricsHolder<>(MyMetrics1.class);
        MetricsHolder<MyMetrics2> group1Holder2 = new MetricsHolder<>(MyMetrics2.class);
        MetricsHolder<MyMetrics1> group2Holder1 = new MetricsHolder<>(MyMetrics1.class);
        MetricsHolder<MyMetrics2> group2Holder2 = new MetricsHolder<>(MyMetrics2.class);

        MetricsGroups.addLastMetricsHolder("myGroup1", group1Holder1);
        MetricsGroups.addLastMetricsHolder("myGroup1", group1Holder2);
        MetricsGroups.addLastMetricsHolder("myGroup2", group2Holder1);
        MetricsGroups.addLastMetricsHolder("myGroup2", group2Holder2);

        MetricsGroup myGroup1 = MetricsGroups.getMetricsGroup("myGroup1");
        LinkedList<MetricsHolder<? extends Metrics>> holders1 = myGroup1.getHolders();
        assertEquals(2, holders1.size());
        assertSame(group1Holder1, holders1.get(0));
        assertSame(group1Holder2, holders1.get(1));

        MetricsGroup myGroup2 = MetricsGroups.getMetricsGroup("myGroup2");
        LinkedList<MetricsHolder<? extends Metrics>> holders2 = myGroup2.getHolders();
        assertEquals(2, holders2.size());
        assertSame(group2Holder1, holders2.get(0));
        assertSame(group2Holder2, holders2.get(1));
    }

    @Test
    public void testStartConcurrently() throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(2);

        List<ScheduledExecutorService> thread1GotExecutors = new ArrayList<>(100);
        List<ScheduledExecutorService> thread2GotExecutors = new ArrayList<>(100);

        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                return;
            }
            for (int i = 0; i < 100; i++) {
                MetricsGroups.start();
                thread1GotExecutors.add(MetricsGroups.getMetricsWriterExecutor());
            }
            stopLatch.countDown();
        }).start();
        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                return;
            }
            for (int i = 0; i < 100; i++) {
                MetricsGroups.start();
                thread2GotExecutors.add(MetricsGroups.getMetricsWriterExecutor());
            }
            stopLatch.countDown();
        }).start();

        startLatch.countDown();
        stopLatch.await();

        assertTrue(MetricsGroups.isStarted());

        ScheduledExecutorService executor = MetricsGroups.getMetricsWriterExecutor();
        for (int i = 0; i < 100; i++) {
            assertSame(executor, thread1GotExecutors.get(i));
            assertSame(executor, thread2GotExecutors.get(i));
        }
    }
}
