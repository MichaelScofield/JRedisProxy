package org.jrp.config;

import org.apache.commons.lang3.BooleanUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Map;

// TODO using Spring to inject ProxyConfig
// TODO supply documents
public class ProxyConfig {

    private static final int DEFAULT_PORT = 16379;

    public static final int DEFAULT_MAX_QUEUED_COMMANDS = 65536;

    public static final boolean PREFER_DIRECT_BYTEBUF =
            BooleanUtils.toBoolean(System.getProperty("redisproxy.internal.bytebuf.preferDirect"));

    private static final int DEFAULT_HANDLER_THREADS_NUM = Runtime.getRuntime().availableProcessors() * 2;

    private static final int DEFAULT_IO_THREADS_NUM = Runtime.getRuntime().availableProcessors();

    public static ProxyConfig loadProxyConfig(String confFile) throws IOException {
        Yaml yaml = new Yaml(new Constructor(ProxyConfig.class));
        Path path = Path.of(confFile);
        InputStream inputStream = Files.newInputStream(path, StandardOpenOption.READ);
        return yaml.load(inputStream);
    }

    // huge command/reply threshold, unit:byte
    private int hugeCommandThreshold = 10240;
    private int hugeReplyThreshold = 20480;

    // the slowlog threshold, unit:MILLISECONDS
    private int slowlogLogSlowerThan = 10;

    private Map<String, String> renameCommands;

    private int port = DEFAULT_PORT;

    private boolean useNettyEpoll = false;

    private int backlog = 512;

    private String redisServerLoader;

    private String name;

    private int ioThreads;

    private int handlerThreads = DEFAULT_HANDLER_THREADS_NUM;

    private boolean readOnly = false;

    private boolean useIdleStateHandler = false;

    private int maxQueuedCommands = DEFAULT_MAX_QUEUED_COMMANDS;

    @Override
    public String toString() {
        return "ProxyConfig{" +
                "hugeCommandThreshold=" + hugeCommandThreshold +
                ", hugeReplyThreshold=" + hugeReplyThreshold +
                ", slowlogLogSlowerThan=" + slowlogLogSlowerThan +
                ", renameCommands=" + renameCommands +
                ", port=" + port +
                ", useNettyEpoll=" + useNettyEpoll +
                ", backlog=" + backlog +
                ", redisServerLoader='" + redisServerLoader + '\'' +
                ", name='" + name + '\'' +
                ", ioThreads=" + ioThreads +
                ", handlerThreads=" + handlerThreads +
                ", readOnly=" + readOnly +
                ", useIdleStateHandler=" + useIdleStateHandler +
                ", maxQueuedCommands=" + maxQueuedCommands +
                '}';
    }

    public int getSlowlogLogSlowerThan() {
        return slowlogLogSlowerThan;
    }

    public void setSlowlogLogSlowerThan(int slowlogLogSlowerThan) {
        this.slowlogLogSlowerThan = slowlogLogSlowerThan;
    }

    public Map<String, String> getRenameCommands() {
        return renameCommands == null ? null : Collections.unmodifiableMap(renameCommands);
    }

    public void setRenameCommands(Map<String, String> renameCommands) {
        this.renameCommands = renameCommands;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isUseNettyEpoll() {
        return useNettyEpoll;
    }

    public void setUseNettyEpoll(boolean useNettyEpoll) {
        this.useNettyEpoll = useNettyEpoll;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public String getRedisServerLoader() {
        return redisServerLoader;
    }

    public void setRedisServerLoader(String redisServerLoader) {
        this.redisServerLoader = redisServerLoader;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getHandlerThreads() {
        return handlerThreads;
    }

    public void setHandlerThreads(int handlerThreads) {
        this.handlerThreads = handlerThreads;
    }

    public int getIoThreads() {
        return ioThreads > 0 ? ioThreads : DEFAULT_IO_THREADS_NUM;
    }

    public void setIoThreads(int ioThreads) {
        this.ioThreads = ioThreads;
    }

    public int getHugeCommandThreshold() {
        return hugeCommandThreshold;
    }

    public void setHugeCommandThreshold(int hugeCommandThreshold) {
        this.hugeCommandThreshold = hugeCommandThreshold;
    }

    public int getHugeReplyThreshold() {
        return hugeReplyThreshold;
    }

    public void setHugeReplyThreshold(int hugeReplyThreshold) {
        this.hugeReplyThreshold = hugeReplyThreshold;
    }

    public boolean isUseIdleStateHandler() {
        return useIdleStateHandler;
    }

    public void setUseIdleStateHandler(boolean useIdleStateHandler) {
        this.useIdleStateHandler = useIdleStateHandler;
    }

    public int getMaxQueuedCommands() {
        return maxQueuedCommands;
    }

    public void setMaxQueuedCommands(int maxQueuedCommands) {
        this.maxQueuedCommands = maxQueuedCommands;
    }
}
