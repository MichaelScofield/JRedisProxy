package org.jrp.monitor.metrics;

import org.jrp.cmd.Command;
import org.jrp.cmd.CommandLifecycle;
import org.jrp.cmd.CommandLifecycle.STATE;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CmdLifecycleMetricsTest {

    @Test
    public void testRecord() throws InterruptedException {
        CmdLifecycleMetrics metrics = new CmdLifecycleMetrics();

        for (int i = 0; i < 10; i++) {
            Command command = new Command();
            CommandLifecycle lifecycle = command.getCommandLifecycle();
            for (STATE state : STATE.values()) {
                lifecycle.setState(state);
                TimeUnit.MICROSECONDS.sleep(10);
            }
            metrics.record(command);
        }

        assertEquals("CMD-QUEUE-PCT    CMD-EXEC-PCT     CMD-FINAL-PCT  ", metrics.getTitle());
        assertEquals(metrics.getTitle().length(), metrics.getStat().length());
    }
}
