package org.jrp.monitor.counter;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LongCounterTest {

    @Test
    public void testCount() {
        long count = RandomUtils.nextLong(0, 10000);
        LongCounter counter = new LongCounter(count);

        for (int i = 0; i < 100; i++) {
            counter.incr();
        }
        assertEquals(count + 100, counter.get());

        for (int i = 0; i < 100; i++) {
            counter.decr();
        }
        assertEquals(count, counter.get());

        counter.incrBy(100);
        assertEquals(count + 100, counter.get());
        counter.incrBy(-100);
        assertEquals(count, counter.get());
    }

    @Test
    public void testCountConcurrently() throws InterruptedException {
        long count = RandomUtils.nextLong(0, 10000);
        LongCounter counter = new LongCounter(count);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(4);

        for (int i = 0; i < 4; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    return;
                }

                for (int c = 0; c < 100; c++) {
                    counter.incr();
                }
                for (int c = 0; c < 100; c++) {
                    counter.decr();
                }
                for (int c = 0; c < 100; c++) {
                    counter.incrBy(100);
                }
                for (int c = 0; c < 100; c++) {
                    counter.incrBy(-100);
                }

                stopLatch.countDown();
            }).start();
        }
        startLatch.countDown();
        stopLatch.await();

        assertEquals(count, counter.get());
    }
}
