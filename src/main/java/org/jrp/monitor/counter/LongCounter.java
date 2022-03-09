package org.jrp.monitor.counter;

import java.util.concurrent.atomic.AtomicLong;

public class LongCounter {

    private final AtomicLong counter;

    public LongCounter(long initialValue) {
        counter = new AtomicLong(initialValue);
    }

    public void incr() {
        counter.getAndIncrement();
    }

    public void incrBy(long l) {
        counter.getAndAdd(l);
    }

    public void decr() {
        counter.getAndDecrement();
    }

    public long get() {
        return counter.get();
    }
}
