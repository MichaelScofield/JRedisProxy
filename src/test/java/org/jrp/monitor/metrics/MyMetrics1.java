package org.jrp.monitor.metrics;

public class MyMetrics1 implements Metrics {

    @Override
    public String getTitle() {
        return "Title1";
    }

    @Override
    public String getStat() {
        return "Stat1";
    }
}
