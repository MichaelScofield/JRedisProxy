package org.jrp;

import com.google.common.base.Joiner;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.IllegalCommandException;
import org.jrp.monitor.CommandTimeMonitor;
import org.jrp.monitor.metrics.CmdLifecycleMetrics;
import org.jrp.monitor.metrics.MetricsGroups;
import org.jrp.monitor.metrics.RedisproxyMetrics;
import org.jrp.monitor.metrics.RedisproxyStatusMetrics;
import org.jrp.server.RedisServer;
import org.jrp.server.RedisproxyAsyncServer;
import org.jrp.server.handler.RedisCommandAsyncHandler;
import org.jrp.server.handler.RedisCommandDecoder;
import org.jrp.server.handler.RedisCommandHandler;
import org.jrp.server.handler.RedisReplyEncoder;
import org.jrp.server.loader.RedisServerLoader;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Bootstrap {

    private static final Logger LOGGER = LogManager.getLogger(Bootstrap.class);

    private final CountDownLatch startLatch = new CountDownLatch(1);

    private final ProxyConfig proxyConfig;
    private final boolean detached;

    private Bootstrap(CommandLine cli) throws IOException {
        this.proxyConfig = ProxyConfig.loadProxyConfig(cli.getOptionValue("c"));
        this.detached = cli.hasOption("detached");
    }

    public Bootstrap(ProxyConfig proxyConfig, boolean detached) {
        this.proxyConfig = proxyConfig;
        this.detached = detached;
    }

    public void start() throws IOException, InterruptedException, IllegalCommandException {
        if (detached) {
            new Thread(() -> {
                try {
                    start0();
                } catch (IOException | IllegalCommandException e) {
                    throw new RuntimeException(e);
                }
            }).start();
            startLatch.await();
        } else {
            start0();
        }
    }

    private void start0() throws IOException, IllegalCommandException {
        LOGGER.info("starting redisproxy with {}", proxyConfig);

        startMonitors();

        String redisServerLoader = proxyConfig.getRedisServerLoader();
        RedisServer redisServer = loadRedisServer(redisServerLoader);

        RedisCommandHandler handler = redisServer instanceof RedisproxyAsyncServer ?
                new RedisCommandAsyncHandler(redisServer, proxyConfig) :
                new RedisCommandHandler(redisServer, proxyConfig);

        startProxy(handler);
    }

    private void startMonitors() {
        String redisproxyId = proxyConfig.getName() + "@" + proxyConfig.getPort();

        String statusMonitorName = "STATUS(" + redisproxyId + ")";
        MetricsGroups.setMetricsLogger(statusMonitorName, LogManager.getLogger("proxy-monitor"));

        MetricsGroups.addFirstMetricsHolder(statusMonitorName, RedisproxyMetrics.HOLDER);
        MetricsGroups.addLastMetricsHolder(statusMonitorName, RedisproxyStatusMetrics.INSTANCE);

        String commandTimeMonitorName = "TIME(" + redisproxyId + ")";
        MetricsGroups.setMetricsLogger(commandTimeMonitorName, LogManager.getLogger("proxy-timeout"));
        MetricsGroups.addFirstMetricsHolder(commandTimeMonitorName, CommandTimeMonitor.HOLDER);

        String cmdLifecyclePctMonitorName = "CMD_LIFECYCLE_PCT(" + redisproxyId + ")";
        MetricsGroups.setMetricsLogger(cmdLifecyclePctMonitorName, LogManager.getLogger("cmd-lifecycle-pct"));
        MetricsGroups.addFirstMetricsHolder(cmdLifecyclePctMonitorName, CmdLifecycleMetrics.HOLDER);

        MetricsGroups.start();
    }

    private void startProxy(RedisCommandHandler handler) {
        int handlerThreads = proxyConfig.getHandlerThreads();
        DefaultEventExecutorGroup group;
        if (handlerThreads > 0) {
            group = new DefaultEventExecutorGroup(handlerThreads);
        } else {
            group = null;
            LOGGER.warn("please be aware that all requests will be executed in IO threads");
        }

        ServerBootstrap bootstrap = new ServerBootstrap();
        EventLoopGroup bossEventLoopGroup;
        EventLoopGroup ioEventLoopGroup;
        if (proxyConfig.isUseNettyEpoll()) {
            bossEventLoopGroup = new EpollEventLoopGroup(1);
            ioEventLoopGroup = new EpollEventLoopGroup(proxyConfig.getIoThreads());
            bootstrap.channel(EpollServerSocketChannel.class);
        } else {
            bossEventLoopGroup = new NioEventLoopGroup(1);
            ioEventLoopGroup = new NioEventLoopGroup(proxyConfig.getIoThreads());
            bootstrap.channel(NioServerSocketChannel.class);
        }
        bootstrap.group(bossEventLoopGroup, ioEventLoopGroup);

        try {
            bootstrap.option(ChannelOption.SO_BACKLOG, proxyConfig.getBacklog())
                    .localAddress(proxyConfig.getPort())
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR,
                            new PooledByteBufAllocator(ProxyConfig.PREFER_DIRECT_BYTEBUF))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ChannelPipeline p = ch.pipeline();
                            if (proxyConfig.isUseIdleStateHandler()) {
                                p.addLast(new IdleStateHandler(
                                        8, 0, 0, TimeUnit.MINUTES));
                            }
                            p.addLast(new RedisCommandDecoder(proxyConfig.getMaxQueuedCommands()));
                            p.addLast(new RedisReplyEncoder());
                            if (group != null) {
                                p.addLast(group, handler);
                            } else {
                                p.addLast(handler);
                            }
                        }
                    });
            ChannelFuture future = bootstrap.bind().sync();

            LOGGER.info("redisproxy started at port {}", proxyConfig.getPort());
            startLatch.countDown();

            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted!", e);
        } finally {
            ioEventLoopGroup.shutdownGracefully();
            bossEventLoopGroup.shutdownGracefully();
        }
    }

    private RedisServer loadRedisServer(String redisServerLoader) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?> redisServerLoaderClass = loadRedisServerClazz(redisServerLoader, classLoader);
        return newInstance(redisServerLoaderClass).load(proxyConfig);
    }

    private Class<?> loadRedisServerClazz(String clazz, ClassLoader classLoader) {
        Class<?> redisServerLoaderClass;
        try {
            redisServerLoaderClass = classLoader.loadClass(clazz);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        if (!RedisServerLoader.class.isAssignableFrom(redisServerLoaderClass)) {
            throw new RuntimeException(clazz + " must extends " + RedisServerLoader.class.getName());
        }
        if (redisServerLoaderClass.isInterface() || Modifier.isAbstract(redisServerLoaderClass.getModifiers())) {
            throw new RuntimeException(clazz + " is not a concrete class!");
        }
        return redisServerLoaderClass;
    }

    private RedisServerLoader newInstance(Class<?> redisServerLoaderClass) {
        Constructor<?> constructor;
        try {
            constructor = redisServerLoaderClass.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        RedisServerLoader redisServerLoader;
        try {
            redisServerLoader = (RedisServerLoader) constructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        return redisServerLoader;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("h", "help", false, "print usage");
        options.addOption("c", "conf", true, "path to config file (required)");
        options.addOption("detached", false, "started in detached mode");

        DefaultParser parser = new DefaultParser();
        CommandLine cli = parser.parse(options, args);
        LOGGER.info("start with options: {}", Joiner.on(";").join(
                Arrays.stream(cli.getOptions())
                        .map(option -> option.getOpt() +
                                (option.getValues() != null ?
                                        "=" + Arrays.toString(option.getValues()) :
                                        ""))
                        .collect(Collectors.toList())));

        if (!cli.hasOption("c") || cli.hasOption("h")) {
            new HelpFormatter().printHelp("Usage:\n", options);
            return;
        }

        Bootstrap bootstrap = new Bootstrap(cli);
        bootstrap.start();
    }
}
