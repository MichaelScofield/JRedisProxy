package org.jrp.server;

import io.netty.channel.Channel;
import org.jrp.cmd.Command;

public final class RedisServerContext {

    private static final ThreadLocal<Command> command = new ThreadLocal<>();

    private static final ThreadLocal<Channel> channel = new ThreadLocal<>();

    public static Command getCommand() {
        return command.get();
    }

    public static Channel getChannel() {
        return channel.get();
    }

    public static void fill(Command cmd, Channel ch) {
        command.set(cmd);
        channel.set(ch);
    }

    public static void clear() {
        command.remove();
        channel.remove();
    }

    private RedisServerContext() {
        // blank
    }
}
