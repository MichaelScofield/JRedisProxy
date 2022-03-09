package org.jrp.monitor.metrics;

import org.jrp.cmd.Command;
import org.jrp.cmd.CommandLifecycle;
import org.jrp.cmd.CommandLifecycle.STATE;
import org.jrp.monitor.counter.CounterFactory;
import org.jrp.monitor.counter.LongCounter;

import java.util.EnumMap;

public class CmdLifecycleMetrics implements Metrics {

    private static final String TITLE = String.format(
            "%-15s  %-15s  %-15s",
            "CMD-QUEUE-PCT", "CMD-EXEC-PCT", "CMD-FINAL-PCT");

    public static final MetricsHolder<CmdLifecycleMetrics> HOLDER = new MetricsHolder<>(CmdLifecycleMetrics.class);

    public static CmdLifecycleMetrics getCurrent() {
        return HOLDER.getCurrent();
    }

    private final EnumMap<STATE, LongCounter> stageMicroseconds;

    public CmdLifecycleMetrics() {
        stageMicroseconds = new EnumMap<>(STATE.class);
        for (STATE state : STATE.values()) {
            stageMicroseconds.put(state, CounterFactory.createLongCounter());
        }
    }

    public void record(Command command) {
        CommandLifecycle lifecycle = command.getCommandLifecycle();
        STATE[] states = STATE.values();
        for (int i = 1; i < states.length; i++) {
            stageMicroseconds.get(states[i])
                    .incrBy(lifecycle.getTimeCostsBetweenInMicroseconds(states[i - 1], states[i]));
        }
    }

    @Override
    public String getTitle() {
        return TITLE;
    }

    @Override
    public String getStat() {
        long newToReadyCosts = stageMicroseconds.get(STATE.READY).get();
        long readyToFinishCosts = stageMicroseconds.get(STATE.FINISH).get();
        long finishToFinalizedCosts = stageMicroseconds.get(STATE.FINALIZE).get();
        long sum = Math.max(newToReadyCosts + readyToFinishCosts + finishToFinalizedCosts, 1);
        return String.format("%-15s  %-15s  %-15s",
                newToReadyCosts * 100 / sum,
                readyToFinishCosts * 100 / sum,
                finishToFinalizedCosts * 100 / sum);
    }
}
