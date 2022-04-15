package org.jrp.monitor.metrics;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.jrp.monitor.counter.CounterFactory;
import org.jrp.monitor.counter.LongCounter;

public class RedisproxyMetrics implements Metrics {

    public static final MetricsHolder<RedisproxyMetrics> HOLDER = new MetricsHolder<>(RedisproxyMetrics.class);

    public static RedisproxyMetrics getCurrent() {
        return HOLDER.getCurrent();
    }

    private static final String FORMAT = "%-8s  %-10s  %-10s  %-10s  %-8s  " +
            "%-8s  %-10s  " +
            "%-10s  %-10s  " +
            "%-10s  %-10s  " +
            "%-8s  %-8s";

    private static final String TITLE = String.format(FORMAT,
            "RECV", "PROC_READ", "PROC_WRITE", "PROC_OTHER", "SENT",
            "DROP_CMD", "DROP_REPLY",
            "BYTES_IN", "BYTES_OUT",
            "ERR_PROTO", "ERR_PROXY",
            "SLOW_EXEC", "CONN_RST");

    public final LongCounter recv;

    public final LongCounter procRead;

    public final LongCounter procWrite;

    public final LongCounter procOther;

    public final LongCounter sent;

    public final LongCounter dropCmd;

    public final LongCounter dropReply;

    public final LongCounter bytesIn;

    public final LongCounter bytesOut;

    public final LongCounter errProtocol;

    public final LongCounter errProxy;

    public final LongCounter slowExec;

    public final LongCounter connReset;

    public RedisproxyMetrics() {
        recv = CounterFactory.createLongCounter();
        procRead = CounterFactory.createLongCounter();
        procWrite = CounterFactory.createLongCounter();
        procOther = CounterFactory.createLongCounter();
        sent = CounterFactory.createLongCounter();
        dropCmd = CounterFactory.createLongCounter();
        dropReply = CounterFactory.createLongCounter();
        bytesIn = CounterFactory.createLongCounter();
        bytesOut = CounterFactory.createLongCounter();
        errProtocol = CounterFactory.createLongCounter();
        errProxy = CounterFactory.createLongCounter();
        slowExec = CounterFactory.createLongCounter();
        connReset = CounterFactory.createLongCounter();
    }

    @Override
    public String getTitle() {
        return TITLE;
    }

    @Override
    public String getStat() {
        return String.format(FORMAT,
                recv.get(), procRead.get(), procWrite.get(), procOther.get(), sent.get(),
                dropCmd.get(), dropReply.get(),
                StringUtils.removeEnd(FileUtils.byteCountToDisplaySize(bytesIn.get()), "bytes"),
                StringUtils.removeEnd(FileUtils.byteCountToDisplaySize(bytesOut.get()), "bytes"),
                errProtocol.get(), errProxy.get(),
                slowExec.get(), connReset.get());
    }
}
