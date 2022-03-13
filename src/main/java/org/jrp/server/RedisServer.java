package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.RedisException;
import org.jrp.reply.*;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

// TODO Split into multiple sub servers (category in Redis command groups).
@SuppressWarnings("unused")
public interface RedisServer extends RedisStringsServer {

    ProxyConfig getProxyConfig();

    @RWType(type = READ)
    Reply bitcount(byte[] key, byte[] start, byte[] end) throws RedisException;

    @RWType(type = WRITE)
    Reply bitop(byte[] operation, byte[] destkey, byte[][] keys) throws RedisException;

    @RWType(type = READ)
    Reply getbit(byte[] key, byte[] offset) throws RedisException;

    @RWType(type = WRITE)
    Reply setbit(byte[] key, byte[] offset, byte[] value) throws RedisException;

    SimpleStringReply auth(byte[] password) throws RedisException;

    @RWType(type = READ)
    BulkReply echo(byte[] message0) throws RedisException;

    @RWType(type = READ)
    SimpleStringReply ping(byte[] message) throws RedisException;

    SimpleStringReply quit() throws RedisException;

    SimpleStringReply select(byte[] index) throws RedisException;

    Reply client(byte[][] args) throws RedisException;

    Reply config(byte[][] args) throws RedisException;

    IntegerReply dbsize() throws RedisException;

    @RWType(type = WRITE)
    SimpleStringReply flushall() throws RedisException;

    @RWType(type = WRITE)
    SimpleStringReply flushdb() throws RedisException;

    Reply info(byte[] section) throws RedisException;

    Reply monitor() throws RedisException;

    Reply slowlog(byte[] subcommand, byte[] argument) throws RedisException;

    @RWType(type = READ)
    MultiBulkReply time() throws RedisException;

    @RWType(type = WRITE)
    MultiBulkReply blpop(byte[][] key) throws RedisException;

    @RWType(type = WRITE)
    MultiBulkReply brpop(byte[][] key) throws RedisException;

    @RWType(type = WRITE)
    BulkReply brpoplpush(byte[] source, byte[] destination, byte[] timeout) throws RedisException;

    @RWType(type = READ)
    Reply lindex(byte[] key, byte[] index) throws RedisException;

    @RWType(type = WRITE)
    Reply linsert(byte[] key, byte[] where, byte[] pivot, byte[] value) throws RedisException;

    @RWType(type = READ)
    Reply llen(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply lpop(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply lpush(byte[] key, byte[][] value) throws RedisException;

    @RWType(type = WRITE)
    Reply lpushx(byte[] key, byte[] value) throws RedisException;

    @RWType(type = READ)
    Reply lrange(byte[] key, byte[] start, byte[] stop) throws RedisException;

    @RWType(type = WRITE)
    Reply lrem(byte[] key, byte[] count, byte[] value) throws RedisException;

    @RWType(type = WRITE)
    Reply lset(byte[] key, byte[] index, byte[] value) throws RedisException;

    @RWType(type = WRITE)
    Reply ltrim(byte[] key, byte[] start, byte[] stop) throws RedisException;

    @RWType(type = WRITE)
    Reply rpop(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply rpoplpush(byte[] source, byte[] destination) throws RedisException;

    @RWType(type = WRITE)
    Reply rpush(byte[] key, byte[][] value) throws RedisException;

    @RWType(type = WRITE)
    Reply rpushx(byte[] key, byte[] value) throws RedisException;

    @RWType(type = WRITE)
    Reply del(byte[][] keys) throws RedisException;

    @RWType(type = READ)
    BulkReply dump(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply exists(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply expire(byte[] key, byte[] seconds) throws RedisException;

    @RWType(type = WRITE)
    Reply expireat(byte[] key, byte[] timestamp) throws RedisException;

    @RWType(type = READ)
    MultiBulkReply keys(byte[] pattern0) throws RedisException;

    @RWType(type = WRITE)
    IntegerReply move(byte[] key, byte[] db) throws RedisException;

    @RWType(type = WRITE)
    Reply persist(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply pexpire(byte[] key, byte[] milliseconds) throws RedisException;

    @RWType(type = WRITE)
    Reply pexpireat(byte[] key, byte[] millisecondsTimestamp) throws RedisException;

    @RWType(type = READ)
    Reply pttl(byte[] key) throws RedisException;

    @RWType(type = READ)
    BulkReply randomkey() throws RedisException;

    @RWType(type = WRITE)
    Reply rename(byte[] key, byte[] newkey) throws RedisException;

    @RWType(type = WRITE)
    Reply renamenx(byte[] key, byte[] newkey) throws RedisException;

    @RWType(type = WRITE)
    SimpleStringReply restore(byte[] key, byte[] ttl, byte[] serializedValue) throws RedisException;

    @RWType(type = READ)
    Reply sort(byte[] key, byte[][] pattern) throws RedisException;

    @RWType(type = READ)
    Reply ttl(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply type(byte[] key) throws RedisException;

    SimpleStringReply unwatch() throws RedisException;

    SimpleStringReply watch(byte[][] key) throws RedisException;

    Reply eval(byte[] script0, byte[] numkeys1, byte[][] key2) throws RedisException;

    Reply evalsha(byte[] sha10, byte[] numkeys1, byte[][] key2) throws RedisException;

    Reply script(byte[][] args) throws RedisException;

    @RWType(type = WRITE)
    Reply hdel(byte[] key, byte[][] field) throws RedisException;

    @RWType(type = READ)
    Reply hexists(byte[] key, byte[] field) throws RedisException;

    @RWType(type = READ)
    Reply hget(byte[] key, byte[] field) throws RedisException;

    @RWType(type = READ)
    Reply hgetall(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply hincrby(byte[] key, byte[] field, byte[] increment) throws RedisException;

    @RWType(type = WRITE)
    Reply hincrbyfloat(byte[] key, byte[] field, byte[] increment) throws RedisException;

    @RWType(type = READ)
    Reply hkeys(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply hlen(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply hmget(byte[] key, byte[][] fields) throws RedisException;

    @RWType(type = WRITE)
    Reply hmset(byte[] key, byte[][] fieldsAndValues) throws RedisException;

    @RWType(type = WRITE)
    Reply hset(byte[] key, byte[][] fieldsAndValues) throws RedisException;

    @RWType(type = WRITE)
    Reply hsetnx(byte[] key, byte[] field, byte[] value) throws RedisException;

    @RWType(type = READ)
    Reply hvals(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply hscan(byte[] key, byte[] cursor, byte[][] attributes) throws RedisException;

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

    @RWType(type = READ)
    Reply scan(byte[] cursor, byte[][] attributes) throws RedisException;

    Reply subscribe(byte[][] channels) throws RedisException;

    Reply command(byte[] subcommand0, byte[][] keys) throws RedisException;

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
