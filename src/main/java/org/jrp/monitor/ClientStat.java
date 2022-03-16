package org.jrp.monitor;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class ClientStat {

    private static final Logger LOGGER = LogManager.getLogger(ClientStat.class);

    private static final AtomicLong CLIENT_ID = new AtomicLong(0);

    private static final Map<ChannelId, ClientStat> CLIENT_STATS = new ConcurrentHashMap<>();

    public final long id;
    private final String addr;
    private int db;

    // TODO complement the following fields,
    //  meaning: https://redis.io/commands/client-list
    //  Some might be unable to provide, just omit and remove them.
    private String laddr;
    private int fd;
    private String name;
    private String flags;
    private int sub;
    private int psub;
    private int multi;
    private int qbuf;
    private int qbufFree;
    private int argvMem;
    private int obl;
    private int oll;
    private int omem;
    private int totalMem;
    private char events = ' ';
    private String cmd;
    private String user;
    private int redir;

    private final long since;
    private long lastActive;
    private int proxyTimeout;

    public static void active(Channel channel) {
        ClientStat stat = new ClientStat(channel);
        CLIENT_STATS.put(channel.id(), stat);
        LOGGER.debug("channel {} is active, create client stat {}", channel, stat);
    }

    public static void inactive(Channel channel) {
        ClientStat stat = CLIENT_STATS.remove(channel.id());
        if (stat != null) {
            LOGGER.debug("channel {} is inactive, remove client stat {}", channel, stat);
        }
    }

    public static void recordActivity(Channel channel) {
        if (!channel.isActive()) {
            return;
        }
        ClientStat stat = CLIENT_STATS.get(channel.id());
        if (stat != null) {
            stat.recordActivity(System.currentTimeMillis());
        }
    }

    public static ClientStat getStat(Channel channel) {
        return CLIENT_STATS.get(channel.id());
    }

    public static String list() {
        return CLIENT_STATS.values().stream()
                .map(ClientStat::dump)
                .collect(Collectors.joining("\n"));
    }

    ClientStat(Channel channel) {
        since = System.currentTimeMillis();
        id = CLIENT_ID.getAndIncrement();
        addr = channel.remoteAddress().toString();
    }

    void recordActivity(long now) {
        lastActive = now;
    }

    public String dump() {
        return "id=" + id + " " +
                "addr=" + addr + " " +
                "laddr=" + laddr + " " +
                "fd=" + fd + " " +
                "name=" + name + " " +
                "age=" + (System.currentTimeMillis() - since) / 1000 + " " +
                "idle=" + (System.currentTimeMillis() - lastActive) / 1000 + " " +
                "flags=" + flags + " " +
                "db=" + db + " " +
                "sub=" + sub + " " +
                "psub=" + psub + " " +
                "multi=" + multi + " " +
                "qbuf=" + qbuf + " " +
                "qbuf-free=" + qbufFree + " " +
                "argv-mem=" + argvMem + " " +
                "obl=" + obl + " " +
                "oll=" + oll + " " +
                "omem=" + omem + " " +
                "tot-mem=" + totalMem + " " +
                "events=" + events + " " +
                "cmd=" + cmd + " " +
                "user=" + user + " " +
                "redir=" + redir;
    }

    public int getProxyTimeout() {
        return proxyTimeout;
    }

    public void setProxyTimeout(int proxyTimeout) {
        this.proxyTimeout = proxyTimeout;
    }

    public void setDb(int db) {
        this.db = db;
    }

    public void setLaddr(String laddr) {
        this.laddr = laddr;
    }

    public void setFd(int fd) {
        this.fd = fd;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFlags(String flags) {
        this.flags = flags;
    }

    public void setSub(int sub) {
        this.sub = sub;
    }

    public void setPsub(int psub) {
        this.psub = psub;
    }

    public void setMulti(int multi) {
        this.multi = multi;
    }

    public void setQbuf(int qbuf) {
        this.qbuf = qbuf;
    }

    public void setQbufFree(int qbufFree) {
        this.qbufFree = qbufFree;
    }

    public void setArgvMem(int argvMem) {
        this.argvMem = argvMem;
    }

    public void setObl(int obl) {
        this.obl = obl;
    }

    public void setOll(int oll) {
        this.oll = oll;
    }

    public void setOmem(int omem) {
        this.omem = omem;
    }

    public void setTotalMem(int totalMem) {
        this.totalMem = totalMem;
    }

    public void setEvents(char events) {
        this.events = events;
    }

    public void setCmd(String cmd) {
        this.cmd = cmd;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setRedir(int redir) {
        this.redir = redir;
    }

    @Override
    public String toString() {
        return "ClientStat(" + dump() + ")";
    }

    @VisibleForTesting
    long getLastActive() {
        return lastActive;
    }

    @VisibleForTesting
    static Map<ChannelId, ClientStat> getClientStats() {
        return CLIENT_STATS;
    }
}
