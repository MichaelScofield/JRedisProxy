package org.jrp.monitor.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;
import java.util.stream.Collectors;

class MetricsGroup {

    private final LinkedList<MetricsHolder<? extends Metrics>> holders = new LinkedList<>();

    private Logger outputLogger = null;

    static String getTitle(Metrics[] metrics) {
        return Arrays.stream(metrics)
                .filter(Objects::nonNull)
                .map(Metrics::getTitle)
                .collect(Collectors.joining("  "));
    }

    static String getStat(Metrics[] metrics) {
        return Arrays.stream(metrics)
                .filter(Objects::nonNull)
                .map(Metrics::getStat)
                .collect(Collectors.joining("  "));
    }

    void addFirstMetricsHolder(MetricsHolder<? extends Metrics> holder) {
        synchronized (holders) {
            holders.addFirst(holder);
        }
    }

    void addLastMetricsHolder(MetricsHolder<? extends Metrics> holder) {
        synchronized (holders) {
            holders.addLast(holder);
        }
    }

    Metrics[] renewMetrics() {
        synchronized (holders) {
            Metrics[] metrics = new Metrics[holders.size()];
            int i = 0;
            for (MetricsHolder<? extends Metrics> holder : holders) {
                metrics[i++] = holder.renewMetrics();
            }
            return metrics;
        }
    }

    Logger getOutputLogger() {
        return outputLogger;
    }

    void setOutputLogger(Logger outputLogger) {
        this.outputLogger = outputLogger;
    }

    @VisibleForTesting
    LinkedList<MetricsHolder<? extends Metrics>> getHolders() {
        return holders;
    }
}
