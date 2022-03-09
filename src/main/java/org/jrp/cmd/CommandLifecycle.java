package org.jrp.cmd;

import com.google.common.annotations.VisibleForTesting;

import static org.jrp.cmd.CommandLifecycle.STATE.NEW;

public class CommandLifecycle {

    public enum STATE {
        NEW, READY, FINISH, FINALIZE
    }

    private volatile STATE state;
    private final long[] timeMicroseconds;

    CommandLifecycle() {
        state = NEW;
        timeMicroseconds = new long[STATE.values().length];
        setState(NEW);
    }

    public void setState(STATE state) {
        this.state = state;
        timeMicroseconds[state.ordinal()] = System.nanoTime();
    }

    public long getTimeCostsBetweenInMicroseconds(STATE state1, STATE state2) {
        return (timeMicroseconds[state2.ordinal()] - timeMicroseconds[state1.ordinal()]) / 1000;
    }

    public long getTimeCostsFromNowInMicroseconds(STATE state) {
        return (System.nanoTime() - timeMicroseconds[state.ordinal()]) / 1000;
    }

    public STATE getState() {
        return state;
    }

    @VisibleForTesting
    long[] getTimeMicroseconds() {
        return timeMicroseconds;
    }
}
