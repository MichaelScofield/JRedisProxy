package org.jrp.monitor;

import com.google.common.annotations.VisibleForTesting;
import org.jrp.monitor.counter.CounterFactory;
import org.jrp.monitor.counter.LongCounter;
import org.jrp.monitor.metrics.Metrics;
import org.jrp.monitor.metrics.MetricsHolder;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CommandTimeMonitor implements Metrics {

    public static final MetricsHolder<CommandTimeMonitor> HOLDER = new MetricsHolder<>(CommandTimeMonitor.class);

    public static CommandTimeMonitor getCurrent() {
        return HOLDER.getCurrent();
    }

    @VisibleForTesting
    static final int[] TIME_COSTS_SCALE_IN_MICRO_SECONDS = new int[]{
            0, 100, 500, // microseconds
            1_000, 5_000, 10_000, 50_000, 100_000, 500_000, // milliseconds
            1_000_000, 2_000_000 // seconds and beyond
    };

    private static final String[] TIME_COST_SCALE_INTERVALS = new String[]{
            "[0,100)", "[100,500)", "[500,1mi)",
            "[1mi,5mi)", "[5mi,10mi)", "[10mi,50mi)", "[50mi,100mi)", "[100mi,500mi)", "[500mi,1s)",
            "[1s,2s)", "[2s,+∞)"
    };

    private final Map<String, LongCounter[]> cmdCounters = new ConcurrentHashMap<>();

    private final Map<String, LongCounter[]> timeCounters = new ConcurrentHashMap<>();

    @Override
    public String getTitle() {
        return "";
    }

    @Override
    public String getStat() {
        String head = String.format(
                "\n%-20s\t%-10s\t%-6s\n",
                "CMD", "TOTAL", "COUNT-TAVG");
        StringBuilder sb = new StringBuilder(head);

        for (Map.Entry<String, LongCounter[]> entry : cmdCounters.entrySet()) {
            String cmd = entry.getKey();
            LongCounter[] timeTotal = timeCounters.get(cmd);
            if (timeTotal == null) {
                continue;
            }

            LongCounter[] cmdTotal = entry.getValue();
            StringBuilder timeRangeStat = new StringBuilder();
            for (int i = 0; i < TIME_COSTS_SCALE_IN_MICRO_SECONDS.length; ++i) {
                long cmdCount = cmdTotal[i].get();
                if (cmdCount > 0) {
                    long timeAvg = timeTotal[i].get() / cmdCount;
                    timeRangeStat.append(TIME_COST_SCALE_INTERVALS[i]).append(":").append(
                            String.format("%s-%s\t", cmdCount, (timeAvg < 1 ? "< 1" : timeAvg)));
                }
            }

            long cmdSum = Arrays.stream(cmdTotal).mapToLong(LongCounter::get).sum();
            sb.append(String.format("%-20s\t%-10d\t%s\n", cmd, cmdSum, timeRangeStat));
        }
        return sb.toString();
    }

    public void addTimeCost(String cmd, int elapsedMicros) {
        int index = Arrays.binarySearch(TIME_COSTS_SCALE_IN_MICRO_SECONDS, elapsedMicros);
        if (index < 0) {
            index = -index - 2; // (-(-insertion point - 1) - 1) - 1
        }

        LongCounter[] timeCostCounter = getCounter(cmd, timeCounters);
        timeCostCounter[index].incrBy(elapsedMicros);

        LongCounter[] cmdCounter = getCounter(cmd, cmdCounters);
        cmdCounter[index].incr();
    }

    @VisibleForTesting
    LongCounter[] getCounter(String cmd, Map<String, LongCounter[]> counters) {
        return counters.computeIfAbsent(cmd, k -> newCounter());
    }

    @VisibleForTesting
    LongCounter[] newCounter() {
        int length = TIME_COSTS_SCALE_IN_MICRO_SECONDS.length;
        LongCounter[] counter = new LongCounter[length];
        for (int i = 0; i < length; ++i) {
            counter[i] = CounterFactory.createLongCounter();
        }
        return counter;
    }

    @VisibleForTesting
    Map<String, LongCounter[]> getCmdCounters() {
        return cmdCounters;
    }

    @VisibleForTesting
    Map<String, LongCounter[]> getTimeCounters() {
        return timeCounters;
    }
}
