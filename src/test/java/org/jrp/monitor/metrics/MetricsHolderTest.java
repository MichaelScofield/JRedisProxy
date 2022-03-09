package org.jrp.monitor.metrics;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

public class MetricsHolderTest {

    @Test
    public void testGetCurrentConcurrently() throws InterruptedException {
        MetricsHolder<MyMetrics1> myMetricsHolder = new MetricsHolder<>(MyMetrics1.class);
        MyMetrics1 myMetrics = myMetricsHolder.getCurrent();
        assertNotNull(myMetrics);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(2);

        List<MyMetrics1> thread1GotMyMetrics = new ArrayList<>(1000);
        List<MyMetrics1> thread2GotMyMetrics = new ArrayList<>(1000);

        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < 1000; i++) {
                thread1GotMyMetrics.add(myMetricsHolder.getCurrent());
            }
            stopLatch.countDown();
        }).start();
        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (int i = 0; i < 1000; i++) {
                thread2GotMyMetrics.add(myMetricsHolder.getCurrent());
            }
            stopLatch.countDown();
        }).start();

        startLatch.countDown();
        stopLatch.await();

        for (int i = 0; i < 1000; i++) {
            assertSame(myMetrics, thread1GotMyMetrics.get(i));
            assertSame(myMetrics, thread2GotMyMetrics.get(i));
        }
    }

    @Test
    public void testRenewMetrics() {
        MetricsHolder<MyMetrics1> myMetricsHolder = new MetricsHolder<>(MyMetrics1.class);
        MyMetrics1 last = myMetricsHolder.getCurrent();
        MyMetrics1 myMetrics = myMetricsHolder.renewMetrics();
        assertSame(last, myMetrics);
        MyMetrics1 curr = myMetricsHolder.getCurrent();
        assertNotSame(last, curr);
    }

    @Test
    public void testNewMetricsInstance() {
        MetricsHolder<MyMetrics1> myMetricsHolder = new MetricsHolder<>(MyMetrics1.class);
        MyMetrics1 myMetrics = myMetricsHolder.newMetricsInstance();
        assertNotNull(myMetrics);
    }
}
