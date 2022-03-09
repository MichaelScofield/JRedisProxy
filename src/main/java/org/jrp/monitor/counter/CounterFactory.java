package org.jrp.monitor.counter;

import java.util.function.Supplier;

public class CounterFactory {

    private static final Supplier<LongCounter> COUNTER_SUPPLIER = () -> new LongCounter(0);

    public static LongCounter createLongCounter() {
        return COUNTER_SUPPLIER.get();
    }
}
