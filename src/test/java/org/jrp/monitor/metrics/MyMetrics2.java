package org.jrp.monitor.metrics;

public class MyMetrics2 implements Metrics {

    @Override
    public String getTitle() {
        return "Title2";
    }

    @Override
    public String getStat() {
        return "Stat2";
    }
}
