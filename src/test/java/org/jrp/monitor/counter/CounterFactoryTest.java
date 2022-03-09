package org.jrp.monitor.counter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotSame;

public class CounterFactoryTest {

    @Test
    public void testCreateLongCounter() {
        LongCounter counter1 = CounterFactory.createLongCounter();
        LongCounter counter2 = CounterFactory.createLongCounter();
        assertNotSame(counter1, counter2);
    }
}
