package org.jrp.server;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.cmd.RedisKeyword;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.RedisException;
import org.jrp.monitor.ClientStat;
import org.jrp.monitor.Monitor;
import org.jrp.reply.*;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.Arrays;

import static org.jrp.exception.RedisException.NOT_IMPLEMENTED_ERROR;
import static org.jrp.reply.SimpleStringReply.*;
import static org.jrp.utils.BytesUtils.string;
import static org.jrp.utils.BytesUtils.toInt;

public abstract class AbstractRedisServer implements RedisServer {

    private static final Logger LOGGER = LogManager.getLogger(AbstractRedisServer.class);

    final ProxyConfig proxyConfig;

    public AbstractRedisServer(ProxyConfig proxyConfig) {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public ProxyConfig getProxyConfig() {
        return proxyConfig;
    }

    @Override
    public Reply append(byte[] key, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply bitcount(byte[] key, byte[] start, byte[] end) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply bitop(byte[] operation, byte[] destkey, byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply decr(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply decrby(byte[] key, byte[] decrement) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply get(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply getdel(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply getex(byte[] key, byte[][] options) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply getbit(byte[] key, byte[] offset) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply incrbyfloat(byte[] key, byte[] increment) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply mget(byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply mset(byte[][] keysAndValues) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply msetnx(byte[][] keysAndValues) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply psetex(byte[] key, byte[] milliseconds, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply set(byte[] key, byte[] value1, byte[][] options) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply setbit(byte[] key, byte[] offset, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply setex(byte[] key, byte[] seconds, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply setnx(byte[] key, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply setrange(byte[] key, byte[] offset, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply strlen(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply substr(byte[] key, byte[] start, byte[] end) throws RedisException {
        return getrange(key, start, end);
    }

    @Override
    public SimpleStringReply auth(byte[] password) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public final BulkReply echo(byte[] message) {
        return BulkReply.bulkReply(message);
    }

    @Override
    public SimpleStringReply select(byte[] index) {
        int db = toInt(index);
        ClientStat stat = ClientStat.getStat(RedisServerContext.getChannel());
        stat.setDb(db);
        return OK;
    }

    @Override
    public final Reply client(byte[][] args) {
        RedisKeyword redisKeyword = RedisKeyword.convert(args[0]);
        if (RedisKeyword.LIST == redisKeyword) {
            LOGGER.warn("start handling \"CLIENT LIST\" command from {}",
                    RedisServerContext.getCommand().getClientAddress());
            return new AsyncReply<>(() -> BulkReply.bulkReply(ClientStat.list()));
        } else {
            return ErrorReply.NYI_REPLY;
        }
    }

    @Override
    public Reply config(byte[][] args) throws RedisException {
        Reply reply;
        RedisKeyword redisKeyword = RedisKeyword.convert(args[0]);
        if (redisKeyword != null) {
            reply = switch (redisKeyword) {
                case GET:
                    yield configGet(args);
                case SET:
                    yield configSet(args);
                default:
                    yield ErrorReply.NYI_REPLY;
            };
        } else {
            reply = ErrorReply.SYNTAX_ERROR;
        }
        return reply;
    }

    private Reply configGet(byte[][] args) throws RedisException {
        String parameter = string(args[1]);
        if (parameter.startsWith("proxy.")) {
            String proxyParameter = StringUtils.removeStart(parameter, "proxy.");
            String value;
            try {
                value = BeanUtils.getProperty(getProxyConfig(), proxyParameter);
            } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new RedisException(e);
            }
            return MultiBulkReply.multiBulkReply(Arrays.asList(parameter, value));
        } else {
            return doConfigGet(parameter);
        }
    }

    protected Reply doConfigGet(String parameter) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    // TODO Select some unchangeable proxy configs such as "port" or "name"
    //  to return Redis error "ERR Unsupported CONFIG parameter: xxx"
    private Reply configSet(byte[][] args) throws RedisException {
        String parameter = string(args[1]);
        String value = string(args[2]);
        if (parameter.startsWith("proxy.")) {
            String proxyParameter = StringUtils.removeStart(parameter, "proxy.");
            if (proxyParameter.equals("timeout")) {
                int proxyTimeout = Integer.parseInt(value);
                ClientStat stat = ClientStat.getStat(RedisServerContext.getChannel());
                stat.setProxyTimeout(proxyTimeout);
            } else {
                try {
                    BeanUtils.setProperty(getProxyConfig(), parameter, value);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RedisException(e);
                }
            }
            return SimpleStringReply.OK;
        } else {
            return doConfigSet(parameter, value);
        }
    }

    protected Reply doConfigSet(String parameter, String value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply info(byte[] section) throws RedisException {
        String sb = "# Proxy Config\n" + getProxyConfig() + "\n" + doInfo(section) + "\n";
        return BulkReply.bulkReply(sb);
    }

    protected String doInfo(byte[] section) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public IntegerReply dbsize() throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public SimpleStringReply flushall() throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public SimpleStringReply flushdb() throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply slowlog(byte[] subcommand, byte[] argument) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public MultiBulkReply time() throws RedisException {
        Instant now = Instant.now();
        long epochSecond = now.getEpochSecond();
        long micros = now.getNano() / 1000 % 1_000_000;
        return MultiBulkReply.multiBulkReply(Arrays.asList(String.valueOf(epochSecond), String.valueOf(micros)));
    }

    @Override
    public MultiBulkReply blpop(byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public MultiBulkReply brpop(byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public BulkReply brpoplpush(byte[] source, byte[] destination, byte[] timeout) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply lindex(byte[] key, byte[] index) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply linsert(byte[] key, byte[] where, byte[] pivot, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply llen(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply lpop(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply lpush(byte[] key, byte[][] values) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply lpushx(byte[] key, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply lrange(byte[] key, byte[] start, byte[] stop) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply lrem(byte[] key, byte[] count, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply lset(byte[] key, byte[] index, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply ltrim(byte[] key, byte[] start, byte[] stop) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply rpop(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply rpoplpush(byte[] source, byte[] destination) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply rpush(byte[] key, byte[][] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply rpushx(byte[] key, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply del(byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public BulkReply dump(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply exists(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply expire(byte[] key, byte[] seconds) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply expireat(byte[] key, byte[] timestamp) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public SimpleStringReply unwatch() throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public SimpleStringReply watch(byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply eval(byte[] script, byte[] numkeys, byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply evalsha(byte[] sha, byte[] numkeys, byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply script(byte[][] args) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hdel(byte[] key, byte[][] fields) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hexists(byte[] key, byte[] field) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hget(byte[] key, byte[] field) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hgetall(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hincrby(byte[] key, byte[] field, byte[] increment) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public MultiBulkReply keys(byte[] pattern) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public IntegerReply move(byte[] key, byte[] db) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply persist(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply pexpire(byte[] key, byte[] milliseconds) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply pexpireat(byte[] key, byte[] millisecondsTs) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply pttl(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public BulkReply randomkey() throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply rename(byte[] key, byte[] newkey) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply renamenx(byte[] key, byte[] newkey) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public SimpleStringReply restore(byte[] key, byte[] ttl, byte[] serializedValue) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sort(byte[] key, byte[][] pattern) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply ttl(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply type(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sdiff(byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sdiffstore(byte[] destination, byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sinter(byte[][] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sinterstore(byte[] destination, byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sismember(byte[] key, byte[] member) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply smembers(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply smove(byte[] source, byte[] destination, byte[] member) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply spop(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply srandmember(byte[] key, byte[] count) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply srem(byte[] key, byte[][] members) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sunion(byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sunionstore(byte[] destination, byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zadd(byte[] key, byte[][] args) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zcard(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zcount(byte[] key, byte[] min, byte[] max) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zincrby(byte[] key, byte[] increment, byte[] member) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zinterstore(byte[] destination, byte[] numkeys, byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zrange(byte[] key, byte[] start, byte[] stop, byte[] withscores) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zrangebyscore(byte[] key, byte[] min, byte[] max, byte[][] args) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zrank(byte[] key0, byte[] member1) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zrem(byte[] key, byte[][] members) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zremrangebyrank(byte[] key, byte[] start, byte[] stop) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zremrangebyscore(byte[] key, byte[] min, byte[] max) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zrevrange(byte[] key, byte[] start, byte[] stop, byte[] withscores) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zrevrangebyscore(byte[] key, byte[] max, byte[] min, byte[][] args) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hincrbyfloat(byte[] key, byte[] field, byte[] increment) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hkeys(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hlen(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hmget(byte[] key, byte[][] fields) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hmset(byte[] key, byte[][] fieldsAndValues) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hset(byte[] key, byte[][] fieldsAndValues) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hsetnx(byte[] key, byte[] field, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hvals(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply hscan(byte[] key, byte[] cursor, byte[][] attributes) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public IntegerReply publish(byte[] channel, byte[] message) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply sadd(byte[] key0, byte[][] members) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply scard(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zrevrank(byte[] key, byte[] member) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zscore(byte[] key0, byte[] member) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zunionstore(byte[] destination, byte[] numkeys, byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply zscan(byte[] key, byte[] cursor, byte[][] attributes) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply scan(byte[] cursor, byte[][] attributes) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply getrange(byte[] key, byte[] start, byte[] end) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply getset(byte[] key, byte[] value) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply incr(byte[] key) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply incrby(byte[] key, byte[] increment) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply subscribe(byte[][] bytes) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply command(byte[] bytes, byte[][] bytes1) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply unsubscribe(byte[][] bytes) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public final Reply monitor() throws RedisException {
        Monitor.startMonitor(RedisServerContext.getChannel());
        return OK;
    }

    @Override
    public final SimpleStringReply quit() {
        return QUIT;
    }

    @Override
    public final SimpleStringReply ping(byte[] message) {
        if (message == null) {
            return PONG;
        } else {
            return new SimpleStringReply(message);
        }
    }

    @Override
    public Reply pfadd(byte[] key, byte[][] elements) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply pfcount(byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply pfmerge(byte[] key, byte[][] keys) throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply multi() throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public Reply exec() throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }

    @Override
    public SimpleStringReply discard() throws RedisException {
        throw NOT_IMPLEMENTED_ERROR;
    }
}
