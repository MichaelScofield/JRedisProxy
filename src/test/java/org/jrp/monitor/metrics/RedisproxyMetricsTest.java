package org.jrp.monitor.metrics;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class RedisproxyMetricsTest {

    @Test
    public void testGetCurrentConcurrently() throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(2);

        List<RedisproxyMetrics> thread1GotObjects = new ArrayList<>(100);
        List<RedisproxyMetrics> thread2GotObjects = new ArrayList<>(100);

        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                return;
            }
            for (int i = 0; i < 100; i++) {
                thread1GotObjects.add(RedisproxyMetrics.getCurrent());
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
                thread2GotObjects.add(RedisproxyMetrics.getCurrent());
            }
            stopLatch.countDown();
        }).start();

        startLatch.countDown();
        stopLatch.await();

        RedisproxyMetrics current = RedisproxyMetrics.getCurrent();
        for (int i = 0; i < 100; i++) {
            assertSame(current, thread1GotObjects.get(i));
            assertSame(current, thread2GotObjects.get(i));
        }
    }

    @Test
    public void testGetTitle() {
        String title = RedisproxyMetrics.getCurrent().getTitle();
        assertEquals("RECV      PROC_READ   PROC_WRITE  PROC_OTHER  SENT      DISCARD   " +
                "BYTES_IN    BYTES_OUT   " +
                "ERR_PROTO   ERR_PROXY   " +
                "SLOW_EXEC  CONN_RST", title);
    }
}
