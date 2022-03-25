package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.RedisException;
import org.jrp.monitor.Monitor;
import org.jrp.reply.*;

import java.time.Instant;
import java.util.Arrays;

import static org.jrp.cmd.RWType.Type.WRITE;
import static org.jrp.reply.SimpleStringReply.*;

// TODO Split into multiple sub servers (category in Redis command groups).
@SuppressWarnings("unused")
public interface RedisServer extends RedisStringServer, RedisBitmapServer, RedisGenericServer,
        RedisListServer, RedisHashServer, RedisSetServer, RedisSortedSetServer {

    ProxyConfig getProxyConfig();

    default Reply client(byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    default BulkReply echo(byte[] message) {
        return BulkReply.bulkReply(message);
    }

    default Reply ping(byte[] message) {
        return message == null ? PONG : BulkReply.bulkReply(message);
    }

    default SimpleStringReply quit() {
        return QUIT;
    }

    default Reply reset() {
        return ErrorReply.NOT_IMPL;
    }

    default Reply select(byte[] index) {
        return ErrorReply.NOT_IMPL;
    }

    default Reply bgrewriteaof() {
        return ErrorReply.NOT_IMPL;
    }

    default Reply bgsave() {
        return ErrorReply.NOT_IMPL;
    }

    default Reply command(byte[] subcommand, byte[][] options) {
        return ErrorReply.NOT_IMPL;
    }

    default Reply config(byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    default Reply dbsize() {
        return ErrorReply.NOT_IMPL;
    }

    default Reply failover(byte[][] options) {
        return ErrorReply.NOT_IMPL;
    }

    default Reply flushall(byte[] option) {
        return ErrorReply.NOT_IMPL;
    }

    default Reply flushdb(byte[] option) {
        return ErrorReply.NOT_IMPL;
    }

    default Reply info(byte[] section) {
        return ErrorReply.NOT_IMPL;
    }

    default Reply monitor() {
        Monitor.startMonitor(RedisServerContext.getChannel());
        return OK;
    }

    default Reply slowlog(byte[] subcommand, byte[] argument) {
        return ErrorReply.NOT_IMPL;
    }

    default MultiBulkReply time() {
        Instant now = Instant.now();
        long epochSecond = now.getEpochSecond();
        long micros = now.getNano() / 1000 % 1_000_000;
        return MultiBulkReply.from(Arrays.asList(String.valueOf(epochSecond), String.valueOf(micros)));
    }

    @RWType(type = WRITE)
    Reply pfadd(byte[] key, byte[][] elements) throws RedisException;

    @RWType(type = WRITE)
    Reply pfcount(byte[][] keys) throws RedisException;

    @RWType(type = WRITE)
    Reply pfmerge(byte[] key, byte[][] keys) throws RedisException;

    Reply multi() throws RedisException;

    Reply exec() throws RedisException;

    SimpleStringReply discard() throws RedisException;
}
