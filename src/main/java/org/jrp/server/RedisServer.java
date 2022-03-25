package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.RedisException;
import org.jrp.monitor.Monitor;
import org.jrp.reply.*;

import java.time.Instant;
import java.util.Arrays;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;
import static org.jrp.reply.SimpleStringReply.*;

// TODO Split into multiple sub servers (category in Redis command groups).
@SuppressWarnings("unused")
public interface RedisServer extends RedisStringServer, RedisBitmapServer, RedisListServer, RedisGenericServer,
        RedisHashServer {

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
    IntegerReply publish(byte[] channel0, byte[] message1) throws RedisException;

    @RWType(type = WRITE)
    Reply sadd(byte[] key, byte[][] member) throws RedisException;

    @RWType(type = READ)
    Reply scard(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply sdiff(byte[][] key) throws RedisException;

    @RWType(type = WRITE)
    Reply sdiffstore(byte[] destination, byte[][] key) throws RedisException;

    @RWType(type = READ)
    Reply sinter(byte[][] key) throws RedisException;

    @RWType(type = WRITE)
    Reply sinterstore(byte[] destination, byte[][] keys) throws RedisException;

    @RWType(type = READ)
    Reply sismember(byte[] key, byte[] member) throws RedisException;

    @RWType(type = READ)
    Reply smembers(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply smove(byte[] source, byte[] destination, byte[] member) throws RedisException;

    @RWType(type = WRITE)
    Reply spop(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply srandmember(byte[] key, byte[] count) throws RedisException;

    @RWType(type = WRITE)
    Reply srem(byte[] key, byte[][] member) throws RedisException;

    @RWType(type = READ)
    Reply sunion(byte[][] key) throws RedisException;

    @RWType(type = WRITE)
    Reply sunionstore(byte[] destination, byte[][] keys) throws RedisException;

    @RWType(type = WRITE)
    Reply zadd(byte[] key, byte[][] args) throws RedisException;

    @RWType(type = READ)
    Reply zcard(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply zcount(byte[] key, byte[] min, byte[] max) throws RedisException;

    @RWType(type = WRITE)
    Reply zincrby(byte[] key, byte[] increment, byte[] member) throws RedisException;

    @RWType(type = WRITE)
    Reply zinterstore(byte[] destination, byte[] numkeys, byte[][] keys) throws RedisException;

    @RWType(type = READ)
    Reply zrange(byte[] key, byte[] start, byte[] stop, byte[] withscores) throws RedisException;

    @RWType(type = READ)
    Reply zrangebyscore(byte[] key, byte[] min, byte[] max, byte[][] args) throws RedisException;

    @RWType(type = READ)
    Reply zrank(byte[] key, byte[] member) throws RedisException;

    @RWType(type = WRITE)
    Reply zrem(byte[] key, byte[][] member) throws RedisException;

    @RWType(type = WRITE)
    Reply zremrangebyrank(byte[] key, byte[] start, byte[] stop) throws RedisException;

    @RWType(type = WRITE)
    Reply zremrangebyscore(byte[] key, byte[] min, byte[] max) throws RedisException;

    @RWType(type = READ)
    Reply zrevrange(byte[] key, byte[] start, byte[] stop, byte[] withscores) throws RedisException;

    @RWType(type = READ)
    Reply zrevrangebyscore(byte[] key, byte[] max, byte[] min, byte[][] args) throws RedisException;

    @RWType(type = READ)
    Reply zrevrank(byte[] key, byte[] member) throws RedisException;

    @RWType(type = READ)
    Reply zscore(byte[] key, byte[] member) throws RedisException;

    @RWType(type = WRITE)
    Reply zunionstore(byte[] destination, byte[] numkeys, byte[][] keys) throws RedisException;

    @RWType(type = READ)
    Reply zscan(byte[] key, byte[] cursor, byte[][] attributes) throws RedisException;

    Reply subscribe(byte[][] channels) throws RedisException;

    Reply unsubscribe(byte[][] channel) throws RedisException;

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
