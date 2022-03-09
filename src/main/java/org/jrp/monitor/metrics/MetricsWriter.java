package org.jrp.monitor.metrics;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Calendar;
import java.util.Map;

class MetricsWriter {

    private static final Logger LOGGER = LogManager.getLogger(MetricsWriter.class);

    private static final String TIME_TITLE = String.format("%-19s  ", "TIME");

    void write(Map<String, MetricsGroup> metricsGroups) {
        try {
            for (Map.Entry<String, MetricsGroup> entry : metricsGroups.entrySet()) {
                MetricsGroup group = entry.getValue();
                Metrics[] metrics = group.renewMetrics();

                Logger outputLogger = group.getOutputLogger();
                if (outputLogger != null) {
                    logMetrics(outputLogger, entry.getKey(), metrics);
                }
            }
        } catch (Throwable e) {
            LOGGER.error("Ops", e);
        }
    }

    private void logMetrics(Logger logger, String metricsName, Metrics[] metrics) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(System.currentTimeMillis());
        String date = DateFormatUtils.format(calendar, "yyyy-MM-dd HH:mm:ss");

        int second = calendar.get(Calendar.SECOND);
        if (second % 10 == 0) {
            String title = MetricsGroup.getTitle(metrics);
            logger.info(metricsName);
            logger.info(TIME_TITLE + title);
        }

        String stat = MetricsGroup.getStat(metrics);
        logger.info(date + "  " + stat);
    }
}
