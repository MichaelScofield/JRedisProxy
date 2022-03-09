package org.jrp.monitor.metrics;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicReference;

public class MetricsHolder<T extends Metrics> {

    private final Class<T> metricsType;

    private final AtomicReference<T> current = new AtomicReference<>();

    public MetricsHolder(Class<T> metricsType) {
        this.metricsType = metricsType;
        renewMetrics();
    }

    public T getCurrent() {
        return current.get();
    }

    T renewMetrics() {
        return current.getAndSet(newMetricsInstance());
    }

    T newMetricsInstance() {
        try {
            Constructor<T> constructor = metricsType.getDeclaredConstructor();
            return constructor.newInstance();
        } catch (Throwable e) {
            throw new RuntimeException(String.format(
                    "unable to create metrics instance for type: %s",
                    metricsType.getSimpleName()), e);
        }
    }
}
