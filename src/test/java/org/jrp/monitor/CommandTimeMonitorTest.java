package org.jrp.monitor;

import org.jrp.monitor.counter.LongCounter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.jrp.monitor.CommandTimeMonitor.TIME_COSTS_SCALE_IN_MICRO_SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class CommandTimeMonitorTest {

    @Test
    public void testGetStat() {
        CommandTimeMonitor monitor = new CommandTimeMonitor();

        LongCounter[] cmdCounters1 = monitor.newCounter();
        for (int i = 0; i < cmdCounters1.length; i++) {
            cmdCounters1[i].incrBy(i + 1);
        }
        LongCounter[] timeCounters1 = monitor.newCounter();
        for (int i = 0; i < timeCounters1.length; i++) {
            long totalCost = cmdCounters1[i].get() * (TIME_COSTS_SCALE_IN_MICRO_SECONDS[i] + 1);
            timeCounters1[i].incrBy(totalCost);
        }
        monitor.getCmdCounters().put("GET", cmdCounters1);
        monitor.getTimeCounters().put("GET", timeCounters1);

        // Test half counters are not printed because there are all zeroes.
        LongCounter[] cmdCounters2 = monitor.newCounter();
        for (int i = 0; i < cmdCounters2.length; i++) {
            if (i % 2 == 0) {
                cmdCounters2[i].incrBy(i + 1);
            }
        }
        LongCounter[] timeCounters2 = monitor.newCounter();
        for (int i = 0; i < timeCounters2.length; i++) {
            if (i % 2 == 0) {
                long totalCost = cmdCounters1[i].get() * (TIME_COSTS_SCALE_IN_MICRO_SECONDS[i] + 1);
                timeCounters2[i].incrBy(totalCost);
            }
        }
        monitor.getCmdCounters().put("SET", cmdCounters2);
        monitor.getTimeCounters().put("SET", timeCounters2);

        String output = monitor.getStat();
        String[] split = output.split("\\n");
        Arrays.sort(split);

        assertEquals("CMD                 \tTOTAL     \tCOUNT-TAVG", split[1]);
        assertEquals("GET                 \t66        \t" +
                "[0,100):1-1\t" +
                "[100,500):2-101\t" +
                "[500,1mi):3-501\t" +
                "[1mi,5mi):4-1001\t" +
                "[5mi,10mi):5-5001\t" +
                "[10mi,50mi):6-10001\t" +
                "[50mi,100mi):7-50001\t" +
                "[100mi,500mi):8-100001\t" +
                "[500mi,1s):9-500001\t" +
                "[1s,2s):10-1000001\t" +
                "[2s,+∞):11-2000001\t", split[2]);
        assertEquals("SET                 \t36        \t" +
                "[0,100):1-1\t" +
                "[500,1mi):3-501\t" +
                "[5mi,10mi):5-5001\t" +
                "[50mi,100mi):7-50001\t" +
                "[500mi,1s):9-500001\t" +
                "[2s,+∞):11-2000001\t", split[3]);
    }

    @Test
    public void testAddTimeCost() {
        CommandTimeMonitor monitor = new CommandTimeMonitor();
        for (int t : TIME_COSTS_SCALE_IN_MICRO_SECONDS) {
            monitor.addTimeCost("PING", t + 1);
        }

        LongCounter[] cmdCounters = monitor.getCmdCounters().get("PING");
        LongCounter[] timeCounters = monitor.getTimeCounters().get("PING");
        int length = TIME_COSTS_SCALE_IN_MICRO_SECONDS.length;
        assertEquals(length, cmdCounters.length);
        assertEquals(length, timeCounters.length);
        for (int i = 0; i < length; i++) {
            assertEquals(1, cmdCounters[i].get());
            assertEquals(TIME_COSTS_SCALE_IN_MICRO_SECONDS[i] + 1, timeCounters[i].get());
        }

        monitor.addTimeCost("PING", 2_000_001);
        assertEquals(2, cmdCounters[length - 1].get());
        assertEquals(TIME_COSTS_SCALE_IN_MICRO_SECONDS[length - 1] + 2_000_002, timeCounters[length - 1].get());
    }

    @Test
    public void testGetCounterConcurrently() throws InterruptedException {
        CommandTimeMonitor monitor = new CommandTimeMonitor();

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch stopLatch = new CountDownLatch(2);

        List<LongCounter[]> thread1GotObjects = new ArrayList<>(100);
        List<LongCounter[]> thread2GotObjects = new ArrayList<>(100);

        new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                return;
            }
            for (int i = 0; i < 100; i++) {
                thread1GotObjects.add(monitor.getCounter("ECHO", monitor.getCmdCounters()));
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
                thread2GotObjects.add(monitor.getCounter("ECHO", monitor.getCmdCounters()));
            }
            stopLatch.countDown();
        }).start();

        startLatch.countDown();
        stopLatch.await();

        LongCounter[] counters = monitor.getCounter("ECHO", monitor.getCmdCounters());
        for (int i = 0; i < 100; i++) {
            assertSame(counters, thread1GotObjects.get(i));
            assertSame(counters, thread2GotObjects.get(i));
        }
    }
}
