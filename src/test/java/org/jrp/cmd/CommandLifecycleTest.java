package org.jrp.cmd;

import org.jrp.cmd.CommandLifecycle.STATE;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CommandLifecycleTest {

    @Test
    public void testInitializedInStateNew() {
        CommandLifecycle lifecycle = new CommandLifecycle();
        assertEquals(STATE.NEW, lifecycle.getState());

        long[] timeMicroseconds = lifecycle.getTimeMicroseconds();
        assertEquals(STATE.values().length, timeMicroseconds.length);
    }

    @Test
    public void testTimeCosts() {
        CommandLifecycle lifecycle = new CommandLifecycle();

        STATE[] states = STATE.values();
        for (STATE state : states) {
            lifecycle.setState(state);
        }

        for (int i = 1; i < states.length; i++) {
            assertTrue(lifecycle.getTimeCostsBetweenInMicroseconds(states[i - 1], states[i]) >= 0);
        }
        for (STATE state : states) {
            assertTrue(lifecycle.getTimeCostsFromNowInMicroseconds(state) >= 0);
        }
    }
}
