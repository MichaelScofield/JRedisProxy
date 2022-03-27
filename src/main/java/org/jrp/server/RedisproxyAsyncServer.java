package org.jrp.server;

import io.lettuce.core.*;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.client.lettuce.LettuceRedisClient;
import org.jrp.cmd.RedisKeyword;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.RedisException;
import org.jrp.reply.*;
import org.jrp.utils.BytesUtils;

import java.util.Set;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static io.lettuce.core.BitFieldArgs.*;
import static org.jrp.cmd.RedisKeyword.*;
import static org.jrp.utils.BytesUtils.*;

public class RedisproxyAsyncServer extends AbstractRedisServer {

    private static final Logger LOGGER = LogManager.getLogger(RedisproxyAsyncServer.class);

    private final LettuceRedisClient lettuceRedisClient;

    public RedisproxyAsyncServer(ProxyConfig proxyConfig, LettuceRedisClient lettuceRedisClient) {
        super(proxyConfig);
        this.lettuceRedisClient = lettuceRedisClient;
    }

    private RedisAsyncCommands<byte[], byte[]> getRedisClient() {
        return lettuceRedisClient.getClient();
    }

    @Override
    public Reply bgrewriteaof() {
        LOGGER.warn("\"BGREWRITEAOF\" was called by {}", RedisServerContext.getChannel());
        RedisFuture<String> future = getRedisClient().bgrewriteaof();
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply bgsave() {
        LOGGER.warn("\"BGSAVE\" was called by {}", RedisServerContext.getChannel());
        RedisFuture<String> future = getRedisClient().bgsave();
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    protected Reply doConfigGet(String parameter) {
        RedisFuture<Map<String, String>> future = getRedisClient().configGet(parameter);
        return new FutureReply<>(future, MultiBulkReply::fromStringMap);
    }

    @Override
    protected Reply doConfigSet(String parameter, String value) {
        RedisFuture<String> future = getRedisClient().configSet(parameter, value);
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    protected Reply configResetstat() {
        RedisFuture<String> future = getRedisClient().configResetstat();
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    protected Reply configRewrite() {
        RedisFuture<String> future = getRedisClient().configRewrite();
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply dbsize() {
        RedisFuture<Long> future = getRedisClient().dbsize();
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply flushall(byte[] option) {
        RedisKeyword keyword = convert(option);
        RedisFuture<String> future = keyword == null ?
                getRedisClient().flushall() :
                getRedisClient().flushall(switch (keyword) {
                    case ASYNC -> FlushMode.ASYNC;
                    case SYNC -> FlushMode.SYNC;
                    default -> throw RedisException.SYNTAX_ERROR;
                });
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply flushdb(byte[] option) {
        RedisKeyword keyword = convert(option);
        RedisFuture<String> future = keyword == null ?
                getRedisClient().flushdb() :
                getRedisClient().flushdb(switch (keyword) {
                    case ASYNC -> FlushMode.ASYNC;
                    case SYNC -> FlushMode.SYNC;
                    default -> throw RedisException.SYNTAX_ERROR;
                });
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    protected String doInfo(byte[] section) {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        RedisFuture<String> future = section == null ? client.info() : client.info(string(section));
        try {
            return future.get(2, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RedisException(e);
        }
    }

    @Override
    public Reply slowlog(byte[] subcommand, byte[] argument) {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        switch (RedisKeyword.convert(subcommand)) {
            case GET -> {
                RedisFuture<List<Object>> future = argument == null ?
                        client.slowlogGet() :
                        client.slowlogGet(toInt(argument));
                return new FutureReply<>(future, MultiBulkReply::from);
            }
            case LEN -> {
                RedisFuture<Long> future = client.slowlogLen();
                return new FutureReply<>(future, IntegerReply::integer);
            }
            case RESET -> {
                RedisFuture<String> future = client.slowlogReset();
                return new FutureReply<>(future, SimpleStringReply::from);
            }
            default -> {
                return ErrorReply.SYNTAX_ERROR;
            }
        }
    }

    @Override
    public Reply append(byte[] key, byte[] value) {
        RedisFuture<Long> future = getRedisClient().append(key, value);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply bitcount(byte[] key, byte[] startBytes, byte[] endBytes) {
        long start = startBytes == null ? 0 : toLong(startBytes);
        long end = endBytes == null ? -1 : toLong(endBytes);
        RedisFuture<Long> future = getRedisClient().bitcount(key, start, end);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply bitfield(byte[] key, byte[][] options) throws RedisException {
        BitFieldArgs args = options == null ? new BitFieldArgs() : parseBitFieldArgs(options);
        RedisFuture<List<Long>> future = getRedisClient().bitfield(key, args);
        return new FutureReply<>(future, MultiBulkReply::from);
    }

    private BitFieldArgs parseBitFieldArgs(byte[][] options) throws RedisException {
        BitFieldArgs args = new BitFieldArgs();
        for (int i = 0; i < options.length; i++) {
            switch (RedisKeyword.convert(options[i])) {
                case GET -> {
                    BitFieldType encoding = parseEncoding(string(options[++i]));
                    Offset offset = parseOffset(string(options[++i]));
                    args.get(encoding, offset);
                }
                case SET -> {
                    BitFieldType encoding = parseEncoding(string(options[++i]));
                    Offset offset = parseOffset(string(options[++i]));
                    long value = toLong(options[++i]);
                    args.set(encoding, offset, value);
                }
                case INCRBY -> {
                    BitFieldType encoding = parseEncoding(string(options[++i]));
                    Offset offset = parseOffset(string(options[++i]));
                    long increment = toLong(options[++i]);
                    args.incrBy(encoding, offset, increment);
                }
                case OVERFLOW -> args.overflow(OverflowType.valueOf(string(options[++i]).toUpperCase()));
            }
        }
        return args;
    }

    private BitFieldType parseEncoding(String encoding) throws RedisException {
        BitFieldType type;
        if (encoding.charAt(0) == 'u') {
            type = unsigned(Integer.parseInt(encoding.substring(1)));
        } else if (encoding.charAt(0) == 'i') {
            type = signed(Integer.parseInt(encoding.substring(1)));
        } else {
            throw RedisException.SYNTAX_ERROR;
        }
        return type;
    }

    private Offset parseOffset(String offset) {
        return offset.charAt(0) == '#' ?
                BitFieldArgs.typeWidthBasedOffset(Integer.parseInt(offset.substring(1))) :
                BitFieldArgs.offset(Integer.parseInt(offset));
    }

    @Override
    public Reply bitop(byte[] operation, byte[] destination, byte[][] keys) throws RedisException {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        String op = string(operation);
        RedisFuture<Long> future = switch (op) {
            case "AND" -> client.bitopAnd(destination, keys);
            case "OR" -> client.bitopOr(destination, keys);
            case "NOT" -> {
                if (keys.length != 1) {
                    throw new RedisException("'BITOP NOT' only allow 1 key");
                }
                yield client.bitopNot(destination, keys[0]);
            }
            case "XOR" -> client.bitopXor(destination, keys);
            default -> throw new IllegalArgumentException(op);
        };
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply bitpos(byte[] key, byte[] bit, byte[] start, byte[] end) {
        RedisFuture<Long> future = getRedisClient().bitpos(key, bit[0] == '1',
                start == null ? 0 : toLong(start), end == null ? -1 : toLong(end));
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply decr(byte[] key) {
        RedisFuture<Long> future = getRedisClient().decr(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply decrby(byte[] key, byte[] decrement) {
        RedisFuture<Long> future = getRedisClient().decrby(key, toLong(decrement));
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply getbit(byte[] key, byte[] offset) {
        RedisFuture<Long> future = getRedisClient().getbit(key, toLong(offset));
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply incrbyfloat(byte[] key, byte[] increment) {
        RedisFuture<Double> future = getRedisClient().incrbyfloat(key, toDouble(increment));
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply mget(byte[][] keys) {
        RedisFuture<List<KeyValue<byte[], byte[]>>> future = getRedisClient().mget(keys);
        return new FutureReply<>(future, MultiBulkReply::fromKeyValues);
    }

    @Override
    public Reply mset(byte[][] keysAndValues) {
        //noinspection DuplicatedCode
        if (keysAndValues.length % 2 != 0) {
            return new ErrorReply("ERR wrong number of arguments for MSET");
        }
        Map<byte[], byte[]> map = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        RedisFuture<String> future = getRedisClient().mset(map);
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply msetnx(byte[][] keysAndValues) {
        //noinspection DuplicatedCode
        if (keysAndValues.length % 2 != 0) {
            return new ErrorReply("ERR wrong number of arguments for MSET");
        }
        Map<byte[], byte[]> map = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        RedisFuture<Boolean> future = getRedisClient().msetnx(map);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply psetex(byte[] rawkey, byte[] milliseconds, byte[] value) {
        RedisFuture<String> future = getRedisClient().psetex(rawkey, toLong(milliseconds), value);
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply set(byte[] key, byte[] value, byte[][] options) throws RedisException {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        RedisFuture<String> future;
        if (options == null) {
            future = client.set(key, value);
        } else {
            SetArgs setArgs = new SetArgs();
            boolean hasExPx = false;
            boolean hasNxXx = false;
            for (int i = 0, l = options.length; i < l; i++) {
                RedisKeyword keyword = RedisKeyword.convert(options[i]);
                if (keyword == EX || keyword == PX || keyword == EXAT || keyword == PXAT || keyword == KEEPTTL) {
                    if (hasExPx) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    switch (keyword) {
                        case EX -> setArgs.ex(toLong(options[++i]));
                        case PX -> setArgs.px(toLong(options[++i]));
                        case EXAT -> setArgs.exAt(toLong(options[++i]));
                        case PXAT -> setArgs.pxAt(toLong(options[++i]));
                        case KEEPTTL -> setArgs.keepttl();
                    }
                    hasExPx = true;
                } else if (keyword == NX || keyword == XX) {
                    if (hasNxXx) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    switch (keyword) {
                        case NX -> setArgs.nx();
                        case XX -> setArgs.xx();
                    }
                    hasNxXx = true;
                } else {
                    throw RedisException.SYNTAX_ERROR;
                }
            }
            future = client.set(key, value, setArgs);
        }
        return new FutureReply<>(future,
                s -> s == null ? BulkReply.NIL_REPLY : SimpleStringReply.from(s));
    }

    @Override
    public Reply setbit(byte[] key, byte[] offset, byte[] value) {
        RedisFuture<Long> future = getRedisClient().setbit(key, toLong(offset), toInt(value));
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply setex(byte[] key, byte[] seconds, byte[] value) {
        RedisFuture<String> future = getRedisClient().setex(key, toLong(seconds), value);
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply setnx(byte[] key, byte[] value) {
        RedisFuture<Boolean> future = getRedisClient().setnx(key, value);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply setrange(byte[] key, byte[] offset, byte[] value) {
        RedisFuture<Long> future = getRedisClient().setrange(key, toLong(offset), value);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply strlen(byte[] key) {
        RedisFuture<Long> future = getRedisClient().strlen(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply lindex(byte[] key, byte[] index) {
        RedisFuture<byte[]> future = getRedisClient().lindex(key, toLong(index));
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply linsert(byte[] key, byte[] where, byte[] pivot, byte[] value) throws RedisException {
        boolean before = switch (RedisKeyword.convert(where)) {
            case BEFORE -> true;
            case AFTER -> false;
            default -> throw RedisException.SYNTAX_ERROR;
        };
        RedisFuture<Long> future = getRedisClient().linsert(key, before, pivot, value);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply llen(byte[] key) {
        RedisFuture<Long> future = getRedisClient().llen(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply lmove(byte[] source, byte[] destination, byte[] whereFrom, byte[] whereTo) {
        RedisKeyword from = RedisKeyword.convert(whereFrom);
        RedisKeyword to = RedisKeyword.convert(whereTo);
        LMoveArgs args;
        if (from == LEFT && to == LEFT) {
            args = LMoveArgs.Builder.leftLeft();
        } else if (from == LEFT && to == RIGHT) {
            args = LMoveArgs.Builder.leftRight();
        } else if (from == RIGHT && to == LEFT) {
            args = LMoveArgs.Builder.rightLeft();
        } else if (from == RIGHT && to == RIGHT) {
            args = LMoveArgs.Builder.rightRight();
        } else {
            return ErrorReply.SYNTAX_ERROR;
        }
        RedisFuture<byte[]> future = getRedisClient().lmove(source, destination, args);
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply lpop(byte[] key, byte[] count) {
        if (count == null) {
            RedisFuture<byte[]> future = getRedisClient().lpop(key);
            return new FutureReply<>(future, BulkReply::bulkReply);
        } else {
            RedisFuture<List<byte[]>> future = getRedisClient().lpop(key, toLong(count));
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    @Override
    public Reply rpop(byte[] key, byte[] count) {
        if (count == null) {
            RedisFuture<byte[]> future = getRedisClient().rpop(key);
            return new FutureReply<>(future, BulkReply::bulkReply);
        } else {
            RedisFuture<List<byte[]>> future = getRedisClient().rpop(key, toLong(count));
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    @Override
    public Reply lpos(byte[] key, byte[] element, byte[][] args) {
        Optional<LPosArgs> lPosArgs = Optional.empty();
        Integer count = null;
        if (args != null) {
            try {
                for (int i = 0; i < args.length; i++) {
                    switch (RedisKeyword.convert(args[i])) {
                        case RANK:
                            lPosArgs = Optional.of(
                                    lPosArgs.orElseGet(LPosArgs.Builder::empty).rank(toLong(args[++i])));
                            break;
                        case COUNT:
                            count = toInt(args[++i]);
                            break;
                        case MAXLEN:
                            lPosArgs = Optional.of(
                                    lPosArgs.orElseGet(LPosArgs.Builder::empty).maxlen(toLong(args[++i])));
                            break;
                        default:
                            return ErrorReply.SYNTAX_ERROR;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("unable to parse LPOS args: " + Arrays.stream(args)
                        .map(BytesUtils::string)
                        .collect(Collectors.joining(",")));
                return ErrorReply.SYNTAX_ERROR;
            }
        }
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        if (count == null) {
            RedisFuture<Long> future = lPosArgs.isPresent() ?
                    client.lpos(key, element, lPosArgs.get()) :
                    client.lpos(key, element);
            return new FutureReply<>(future, IntegerReply::new);
        } else {
            RedisFuture<List<Long>> future = lPosArgs.isPresent() ?
                    client.lpos(key, element, count, lPosArgs.get()) :
                    client.lpos(key, element, count);
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    @Override
    public Reply lpush(byte[] key, byte[][] elements) {
        RedisFuture<Long> future = getRedisClient().lpush(key, elements);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply rpush(byte[] key, byte[][] elements) {
        RedisFuture<Long> future = getRedisClient().rpush(key, elements);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply lpushx(byte[] key, byte[][] elements) {
        RedisFuture<Long> future = getRedisClient().lpushx(key, elements);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply rpushx(byte[] key, byte[][] elements) {
        RedisFuture<Long> future = getRedisClient().rpushx(key, elements);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply lrange(byte[] key, byte[] start, byte[] stop) {
        RedisFuture<List<byte[]>> future = getRedisClient().lrange(key, toLong(start), toLong(stop));
        return new FutureReply<>(future, MultiBulkReply::from);
    }

    @Override
    public Reply lrem(byte[] key, byte[] count, byte[] element) {
        RedisFuture<Long> future = getRedisClient().lrem(key, toLong(count), element);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply lset(byte[] key, byte[] index, byte[] element) {
        RedisFuture<String> future = getRedisClient().lset(key, toLong(index), element);
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply ltrim(byte[] key, byte[] start, byte[] stop) {
        RedisFuture<String> future = getRedisClient().ltrim(key, toLong(start), toLong(stop));
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply rpoplpush(byte[] source, byte[] destination) {
        RedisFuture<byte[]> future = getRedisClient().rpoplpush(source, destination);
        return new FutureReply<>(future, BulkReply::new);
    }

    @Override
    public Reply del(byte[][] keys) {
        RedisFuture<Long> future = getRedisClient().del(keys);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply exists(byte[][] keys) {
        RedisFuture<Long> future = getRedisClient().exists(keys);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply expire(byte[] key, byte[] seconds) {
        RedisFuture<Boolean> future = getRedisClient().expire(key, toLong(seconds));
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply expireat(byte[] key, byte[] timestamp) {
        RedisFuture<Boolean> future = getRedisClient().expireat(key, toLong(timestamp));
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply hdel(byte[] key, byte[][] fields) {
        RedisFuture<Long> future = getRedisClient().hdel(key, fields);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply hexists(byte[] key, byte[] field) {
        RedisFuture<Boolean> future = getRedisClient().hexists(key, field);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply hget(byte[] key, byte[] field) {
        RedisFuture<byte[]> future = getRedisClient().hget(key, field);
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply hgetall(byte[] key) {
        RedisFuture<Map<byte[], byte[]>> future = getRedisClient().hgetall(key);
        return new FutureReply<>(future, MultiBulkReply::fromBytesMap);
    }

    @Override
    public Reply hincrby(byte[] key, byte[] field, byte[] increment) {
        RedisFuture<Long> future = getRedisClient().hincrby(key, field, toLong(increment));
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply persist(byte[] key) {
        RedisFuture<Boolean> future = getRedisClient().persist(key);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply pexpire(byte[] key, byte[] milliseconds) {
        RedisFuture<Boolean> future = getRedisClient().pexpire(key, toLong(milliseconds));
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply pexpireat(byte[] key, byte[] millisecondsTimestamp) {
        RedisFuture<Boolean> future = getRedisClient().pexpireat(key, toLong(millisecondsTimestamp));
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply pttl(byte[] key) {
        RedisFuture<Long> future = getRedisClient().pttl(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply rename(byte[] key, byte[] newkey) {
        RedisFuture<String> future = getRedisClient().rename(key, newkey);
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply renamenx(byte[] key, byte[] newkey) {
        RedisFuture<Boolean> future = getRedisClient().renamenx(key, newkey);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply sort(byte[] key, byte[][] args) throws RedisException {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        if (args == null) {
            RedisFuture<List<byte[]>> future = client.sort(key);
            return new FutureReply<>(future, MultiBulkReply::from);
        } else {
            Pair<byte[], SortArgs> pair = getSortArgs(args);
            byte[] destination = pair.getLeft();
            SortArgs sortArgs = pair.getRight();
            if (destination == null) {
                RedisFuture<List<byte[]>> future = client.sort(key, sortArgs);
                return new FutureReply<>(future, MultiBulkReply::from);
            } else {
                RedisFuture<Long> future = client.sortStore(key, sortArgs, destination);
                return new FutureReply<>(future, IntegerReply::new);
            }
        }
    }

    private Pair<byte[], SortArgs> getSortArgs(byte[][] pattern) throws RedisException {
        byte[] destination = null;
        SortArgs sortArgs = new SortArgs();
        for (int i = 0, l = pattern.length; i < l; i++) {
            byte[] option = pattern[i];
            switch (RedisKeyword.convert(option)) {
                case BY -> {
                    if (++i >= l) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    sortArgs.by(string(pattern[i]));
                }
                case LIMIT -> {
                    if (++i >= l) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    int offset = toInt(pattern[i]);
                    if (++i >= l) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    int count = toInt(pattern[i]);
                    sortArgs.limit(offset, count);
                }
                case GET -> {
                    if (++i >= l) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    sortArgs.get(string(pattern[i]));
                }
                case ASC -> sortArgs.asc();
                case DESC -> sortArgs.desc();
                case ALPHA -> sortArgs.alpha();
                case STORE -> {
                    if (++i >= l) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    destination = pattern[i];
                }
                default -> throw RedisException.SYNTAX_ERROR;
            }
        }
        return Pair.of(destination, sortArgs);
    }

    @Override
    public Reply ttl(byte[] key) {
        RedisFuture<Long> future = getRedisClient().ttl(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply type(byte[] key) {
        RedisFuture<String> future = getRedisClient().type(key);
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply sdiff(byte[][] keys) {
        RedisFuture<Set<byte[]>> future = getRedisClient().sdiff(keys);
        return new FutureReply<>(future, MultiBulkReply::from);
    }

    @Override
    public Reply sdiffstore(byte[] destination, byte[][] keys) {
        RedisFuture<Long> future = getRedisClient().sdiffstore(destination, keys);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply sinter(byte[][] keys) {
        RedisFuture<Set<byte[]>> future = getRedisClient().sinter(keys);
        return new FutureReply<>(future, MultiBulkReply::from);
    }

    @Override
    public Reply sinterstore(byte[] destination, byte[][] keys) {
        RedisFuture<Long> future = getRedisClient().sinterstore(destination, keys);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply sismember(byte[] key, byte[] member) {
        RedisFuture<Boolean> future = getRedisClient().sismember(key, member);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply smembers(byte[] key) {
        RedisFuture<Set<byte[]>> future = getRedisClient().smembers(key);
        return new FutureReply<>(future, MultiBulkReply::from);
    }

    @Override
    public Reply smove(byte[] source, byte[] destination, byte[] member) {
        RedisFuture<Boolean> future = getRedisClient().smove(source, destination, member);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply spop(byte[] key, byte[] count) {
        if (count == null) {
            RedisFuture<byte[]> future = getRedisClient().spop(key);
            return new FutureReply<>(future, BulkReply::bulkReply);
        } else {
            RedisFuture<Set<byte[]>> future = getRedisClient().spop(key, toLong(count));
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    @Override
    public Reply srandmember(byte[] key, byte[] count) {
        if (count == null) {
            RedisFuture<byte[]> future = getRedisClient().srandmember(key);
            return new FutureReply<>(future, BulkReply::bulkReply);
        } else {
            RedisFuture<List<byte[]>> future = getRedisClient().srandmember(key, toLong(count));
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    @Override
    public Reply srem(byte[] key, byte[][] members) {
        RedisFuture<Long> future = getRedisClient().srem(key, members);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply sunion(byte[][] keys) {
        RedisFuture<Set<byte[]>> future = getRedisClient().sunion(keys);
        return new FutureReply<>(future, MultiBulkReply::from);
    }

    @Override
    public Reply sunionstore(byte[] destination, byte[][] keys) {
        RedisFuture<Long> future = getRedisClient().sunionstore(destination, keys);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply zadd(byte[] key, byte[][] args) {
        ZAddArgs zAddArgs = new ZAddArgs();
        List<ScoredValue<byte[]>> scoreValues = new ArrayList<>();
        boolean hasNX = false;
        boolean hasXX = false;
        boolean hasGTorLT = false;
        for (int i = 0; i < args.length; i++) {
            try {
                RedisKeyword keyword = convert(args[i]);
                if (keyword == null) {
                    double score = toDouble(args[i]);
                    byte[] member = args[++i];
                    scoreValues.add(ScoredValue.just(score, member));
                } else {
                    switch (keyword) {
                        case NX -> {
                            if (hasNX || hasXX) {
                                return ErrorReply.SYNTAX_ERROR;
                            }
                            zAddArgs.nx();
                            hasNX = true;
                        }
                        case XX -> {
                            if (hasNX || hasXX) {
                                return ErrorReply.SYNTAX_ERROR;
                            }
                            zAddArgs.xx();
                            hasXX = true;
                        }
                        case GT -> {
                            if (hasGTorLT || hasNX) {
                                return ErrorReply.SYNTAX_ERROR;
                            }
                            zAddArgs.gt();
                            hasGTorLT = true;
                        }
                        case LT -> {
                            if (hasGTorLT || hasNX) {
                                return ErrorReply.SYNTAX_ERROR;
                            }
                            zAddArgs.lt();
                            hasGTorLT = true;
                        }
                        case CH -> zAddArgs.ch();
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("unable to parse ZADD args: {}",
                        Arrays.stream(args).map(BytesUtils::string).collect(Collectors.joining(", ")));
                return ErrorReply.SYNTAX_ERROR;
            }
        }
        //noinspection unchecked
        ScoredValue<byte[]>[] scoredValuesArr = scoreValues.toArray(ScoredValue[]::new);
        RedisFuture<Long> future = getRedisClient().zadd(key, zAddArgs, scoredValuesArr);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply zcard(byte[] key) {
        RedisFuture<Long> future = getRedisClient().zcard(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply zcount(byte[] key, byte[] min, byte[] max) {
        Range<Number> range = createRange(min, max);
        RedisFuture<Long> future = getRedisClient().zcount(key, range);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply zincrby(byte[] key, byte[] increment, byte[] member) {
        RedisFuture<Double> future = getRedisClient().zincrby(key, toDouble(increment), member);
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply zinterstore(byte[] destination, byte[] numkeysBytes, byte[][] args) throws RedisException {
        int numkeys = toInt(numkeysBytes);
        ZStoreArgs zStoreArgs = getZStoreArgs(numkeys, args);
        byte[][] keys = new byte[numkeys][];
        System.arraycopy(args, 0, keys, 0, numkeys);
        RedisFuture<Long> future = getRedisClient().zinterstore(destination, zStoreArgs, keys);
        return new FutureReply<>(future, IntegerReply::new);
    }

    private ZStoreArgs getZStoreArgs(int numkeys, byte[][] args) throws RedisException {
        ZStoreArgs zStoreArgs = new ZStoreArgs();
        for (int i = numkeys, l = args.length; i < l; ++i) {
            byte[] bytes = args[i];
            RedisKeyword option = RedisKeyword.convert(bytes);
            switch (option) {
                case WEIGHTS -> {
                    double[] weights = new double[numkeys];
                    for (int j = 0; j < numkeys; ++j) {
                        if (++i >= l) {
                            throw RedisException.SYNTAX_ERROR;
                        }
                        weights[j] = toDouble(args[i]);
                    }
                    zStoreArgs.weights(weights);
                }
                case AGGREGATE -> {
                    if (++i >= l) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    RedisKeyword aggregateOption = RedisKeyword.convert(args[i]);
                    if (aggregateOption == null) {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    switch (aggregateOption) {
                        case SUM -> zStoreArgs.sum();
                        case MIN -> zStoreArgs.min();
                        case MAX -> zStoreArgs.max();
                        default -> throw RedisException.SYNTAX_ERROR;
                    }
                }
                default -> throw RedisException.SYNTAX_ERROR;
            }
        }
        return zStoreArgs;
    }

    // TODO lack of unit tests
    @Override
    public Reply zrange(byte[] rawkey, byte[] startBytes, byte[] stopBytes, byte[] withScores) {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        long start = toLong(startBytes);
        long stop = toLong(stopBytes);
        if (RedisKeyword.convert(withScores) == RedisKeyword.WITHSCORES) {
            RedisFuture<List<ScoredValue<byte[]>>> future = client.zrangeWithScores(rawkey, start, stop);
            return new FutureReply<>(future, MultiBulkReply::fromScoreValues);
        } else {
            RedisFuture<List<byte[]>> future = client.zrange(rawkey, start, stop);
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    private record ScoreAttributes(boolean limit, boolean withScores, int offset, int count) {
    }

    private ScoreAttributes toScoreAttributes(byte[][] args) throws RedisException {
        boolean isWithScores = false;
        boolean hasLimit = false;
        int offset = 0;
        int count = Integer.MAX_VALUE;
        for (int i = 0; i < args.length; i++) {
            byte[] option = args[i];
            RedisKeyword redisKeyword = RedisKeyword.convert(option);
            switch (redisKeyword) {
                case WITHSCORES -> isWithScores = true;
                case LIMIT -> {
                    hasLimit = true;
                    if (++i >= args.length) {
                        throw new RedisException("syntax error");
                    }
                    offset = toInt(args[i]);
                    if (++i >= args.length) {
                        throw new RedisException("syntax error");
                    }
                    count = toInt(args[i]);
                }
                default -> throw new RedisException("syntax error");
            }
        }
        return new ScoreAttributes(hasLimit, isWithScores, offset, count);
    }

    // TODO lack of unit tests
    // TODO parse range like min = "(2"
    @Override
    public Reply zrangebyscore(byte[] rawkey, byte[] min, byte[] max, byte[][] args) throws RedisException {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        Range<Double> range = Range.create(toDouble(min), toDouble(max));
        if (args == null) {
            RedisFuture<List<byte[]>> future = client.zrangebyscore(rawkey, range);
            return new FutureReply<>(future, MultiBulkReply::from);
        }

        ScoreAttributes scoreAttributes = toScoreAttributes(args);
        if (scoreAttributes.withScores()) {
            RedisFuture<List<ScoredValue<byte[]>>> future;
            if (scoreAttributes.limit()) {
                future = client.zrangebyscoreWithScores(rawkey, range,
                        Limit.create(scoreAttributes.offset(), scoreAttributes.count()));
            } else {
                future = client.zrangebyscoreWithScores(rawkey, range);
            }
            return new FutureReply<>(future, MultiBulkReply::fromScoreValues);
        } else {
            RedisFuture<List<byte[]>> future;
            if (scoreAttributes.limit()) {
                future = client.zrangebyscore(rawkey, range,
                        Limit.create(scoreAttributes.offset(), scoreAttributes.count()));
            } else {
                future = client.zrangebyscore(rawkey, range);
            }
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    @Override
    public Reply zrank(byte[] key, byte[] member) {
        RedisFuture<Long> future = getRedisClient().zrank(key, member);
        return new FutureReply<>(future, i -> i == null ? BulkReply.NIL_REPLY : IntegerReply.integer(i));
    }

    @Override
    public Reply zrem(byte[] key, byte[][] members) {
        RedisFuture<Long> future = getRedisClient().zrem(key, members);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply zremrangebyrank(byte[] key, byte[] start, byte[] stop) {
        RedisFuture<Long> future = getRedisClient().zremrangebyrank(key, toLong(start), toLong(stop));
        return new FutureReply<>(future, IntegerReply::new);
    }

    // TODO parse range like min = "(2"
    @Override
    public Reply zremrangebyscore(byte[] key, byte[] min, byte[] max) {
        Range<Double> range = Range.create(toDouble(min), toDouble(max));
        RedisFuture<Long> future = getRedisClient().zremrangebyscore(key, range);
        return new FutureReply<>(future, IntegerReply::new);
    }

    // TODO replace with zrange: https://redis.io/commands/zrevrange/
    @Override
    public Reply zrevrange(byte[] rawkey, byte[] startBytes, byte[] stopBytes, byte[] withScores) {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        long start = toLong(startBytes);
        long stop = toLong(stopBytes);
        if (RedisKeyword.convert(withScores) == RedisKeyword.WITHSCORES) {
            RedisFuture<List<ScoredValue<byte[]>>> future = client.zrevrangeWithScores(rawkey, start, stop);
            return new FutureReply<>(future, MultiBulkReply::fromScoreValues);
        } else {
            RedisFuture<List<byte[]>> future = client.zrevrange(rawkey, start, stop);
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    // TODO lack of unit tests
    // TODO parse range like min = "(2"
    @Override
    public Reply zrevrangebyscore(byte[] key, byte[] max, byte[] min, byte[][] args) throws RedisException {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        Range<Double> range = Range.create(toDouble(min), toDouble(max));
        if (args == null) {
            RedisFuture<List<byte[]>> future = client.zrevrangebyscore(key, range);
            return new FutureReply<>(future, MultiBulkReply::from);
        }

        ScoreAttributes scoreAttributes = toScoreAttributes(args);
        if (scoreAttributes.withScores()) {
            RedisFuture<List<ScoredValue<byte[]>>> future;
            if (scoreAttributes.limit()) {
                future = client.zrevrangebyscoreWithScores(key, range,
                        Limit.create(scoreAttributes.offset(), scoreAttributes.count()));
            } else {
                future = client.zrevrangebyscoreWithScores(key, range);
            }
            return new FutureReply<>(future, MultiBulkReply::fromScoreValues);
        } else {
            RedisFuture<List<byte[]>> future;
            if (scoreAttributes.limit()) {
                future = client.zrevrangebyscore(key, range,
                        Limit.create(scoreAttributes.offset(), scoreAttributes.count()));
            } else {
                future = client.zrevrangebyscore(key, range);
            }
            return new FutureReply<>(future, MultiBulkReply::from);
        }
    }

    @Override
    public Reply hincrbyfloat(byte[] key, byte[] field, byte[] increment) {
        RedisFuture<Double> future = getRedisClient().hincrbyfloat(key, field, toDouble(increment));
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply hkeys(byte[] key) {
        RedisFuture<List<byte[]>> future = getRedisClient().hkeys(key);
        return new FutureReply<>(future, MultiBulkReply::from);
    }

    @Override
    public Reply hlen(byte[] key) {
        RedisFuture<Long> future = getRedisClient().hlen(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply hmget(byte[] key, byte[][] fields) {
        RedisFuture<List<KeyValue<byte[], byte[]>>> future = getRedisClient().hmget(key, fields);
        return new FutureReply<>(future, MultiBulkReply::fromKeyValues);
    }

    @Override
    public Reply hmset(byte[] key, byte[][] fieldsAndValues) {
        Map<byte[], byte[]> map = new HashMap<>();
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            map.put(fieldsAndValues[i], fieldsAndValues[i + 1]);
        }
        RedisFuture<String> future = getRedisClient().hmset(key, map);
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply hset(byte[] key, byte[][] fieldsAndValues) {
        Map<byte[], byte[]> map = new HashMap<>();
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            map.put(fieldsAndValues[i], fieldsAndValues[i + 1]);
        }
        RedisFuture<Long> future = getRedisClient().hset(key, map);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply hsetnx(byte[] key, byte[] field, byte[] value) {
        RedisFuture<Boolean> future = getRedisClient().hsetnx(key, field, value);
        return new FutureReply<>(future, IntegerReply::integer);
    }

    @Override
    public Reply hvals(byte[] key) {
        RedisFuture<List<byte[]>> future = getRedisClient().hvals(key);
        return new FutureReply<>(future, MultiBulkReply::from);
    }

    // TODO lack of unit tests
    @Override
    public Reply hscan(byte[] key, byte[] cursor, byte[][] args) throws RedisException {
        ScanCursor scanCursor = new ScanCursor(string(cursor), false);
        ScanArgs scanArgs = getScanArgs(args);
        RedisFuture<MapScanCursor<byte[], byte[]>> future =
                getRedisClient().hscan(key, scanCursor, scanArgs);
        return new FutureReply<>(future, mapScanCursor -> new MultiBulkReply(new Reply[]{
                BulkReply.bulkReply(mapScanCursor.getCursor()),
                MultiBulkReply.fromBytesMap(mapScanCursor.getMap())}));
    }

    private ScanArgs getScanArgs(byte[][] attributes) throws RedisException {
        ScanArgs scanArgs = new ScanArgs();
        if (attributes == null) {
            return scanArgs;
        }
        for (int i = 0, l = attributes.length; i < l; i++) {
            String attribute = string(attributes[i]).toUpperCase();
            switch (attribute) {
                case "MATCH":
                    if (i + 1 < l) {
                        scanArgs.match(string(attributes[++i]));
                    } else {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    break;
                case "COUNT":
                    if (i + 1 < l) {
                        scanArgs.limit(toInt(attributes[++i]));
                    } else {
                        throw RedisException.SYNTAX_ERROR;
                    }
                    break;
                default:
                    throw RedisException.SYNTAX_ERROR;
            }
        }
        return scanArgs;
    }

    @Override
    public Reply sadd(byte[] key, byte[][] members) {
        RedisFuture<Long> future = getRedisClient().sadd(key, members);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply scard(byte[] key) {
        RedisFuture<Long> future = getRedisClient().scard(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply zrevrank(byte[] key, byte[] member) {
        RedisFuture<Long> future = getRedisClient().zrevrank(key, member);
        return new FutureReply<>(future, i -> i == null ? BulkReply.NIL_REPLY : IntegerReply.integer(i));
    }

    @Override
    public Reply zscore(byte[] key, byte[] member) {
        RedisFuture<Double> future = getRedisClient().zscore(key, member);
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    // TODO lack of unit tests
    @Override
    public Reply zunionstore(byte[] destination, byte[] numkeysBytes, byte[][] args) throws RedisException {
        RedisAsyncCommands<byte[], byte[]> client = getRedisClient();
        int numkeys = toInt(numkeysBytes);
        RedisFuture<Long> future;
        if (args.length == numkeys) {
            future = client.zunionstore(destination, args);
        } else if (args.length > numkeys) {
            ZStoreArgs zStoreArgs = getZStoreArgs(numkeys, args);
            byte[][] keys = new byte[numkeys][];
            System.arraycopy(args, 0, keys, 0, numkeys);
            future = client.zunionstore(destination, zStoreArgs, keys);
        } else {
            throw RedisException.SYNTAX_ERROR;
        }
        return new FutureReply<>(future, IntegerReply::new);
    }

    // TODO lack of unit tests
    @Override
    public Reply zscan(byte[] key, byte[] cursor, byte[][] attributes) throws RedisException {
        ScanCursor scanCursor = new ScanCursor(string(cursor), false);
        ScanArgs scanArgs = getScanArgs(attributes);
        RedisFuture<ScoredValueScanCursor<byte[]>> future = getRedisClient().zscan(key, scanCursor, scanArgs);
        return new FutureReply<>(future, mapScanCursor -> new MultiBulkReply(new Reply[]{
                BulkReply.bulkReply(mapScanCursor.getCursor()),
                MultiBulkReply.fromScoreValues(mapScanCursor.getValues())}));
    }

    @Override
    public Reply getrange(byte[] key, byte[] start, byte[] end) {
        RedisFuture<byte[]> future = getRedisClient().getrange(key, toLong(start), toLong(end));
        return new FutureReply<>(future, BulkReply::new);
    }

    @Override
    public Reply getset(byte[] key, byte[] value) {
        RedisFuture<byte[]> future = getRedisClient().getset(key, value);
        return new FutureReply<>(future, BulkReply::new);
    }

    @Override
    public Reply incr(byte[] key) {
        RedisFuture<Long> future = getRedisClient().incr(key);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply incrby(byte[] key, byte[] increment) {
        RedisFuture<Long> future = getRedisClient().incrby(key, toLong(increment));
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply pfadd(byte[] key, byte[][] elements) {
        RedisFuture<Long> future = getRedisClient().pfadd(key, elements);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply pfcount(byte[][] keys) {
        RedisFuture<Long> future = getRedisClient().pfcount(keys);
        return new FutureReply<>(future, IntegerReply::new);
    }

    @Override
    public Reply pfmerge(byte[] destkey, byte[][] sourceKeys) {
        RedisFuture<String> future = getRedisClient().pfmerge(destkey, sourceKeys);
        return new FutureReply<>(future, SimpleStringReply::from);
    }

    @Override
    public Reply get(byte[] key) {
        RedisFuture<byte[]> future = getRedisClient().get(key);
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply getdel(byte[] key) {
        RedisFuture<byte[]> future = getRedisClient().getdel(key);
        return new FutureReply<>(future, BulkReply::bulkReply);
    }

    @Override
    public Reply getex(byte[] key, byte[][] options) {
        GetExArgs args = new GetExArgs();
        if (options != null) {
            if (options.length == 1) {
                if (RedisKeyword.convert(options[0]) != RedisKeyword.PERSIST) {
                    return ErrorReply.SYNTAX_ERROR;
                }
                args = GetExArgs.Builder.persist();
            } else if (options.length == 2) {
                switch (RedisKeyword.convert(options[0])) {
                    case EX -> args = GetExArgs.Builder.ex(toLong(options[1]));
                    case PX -> args = GetExArgs.Builder.px(toLong(options[1]));
                    case EXAT -> args = GetExArgs.Builder.exAt(toLong(options[1]));
                    case PXAT -> args = GetExArgs.Builder.pxAt(toLong(options[1]));
                    default -> {
                        return ErrorReply.SYNTAX_ERROR;
                    }
                }
            } else {
                return ErrorReply.SYNTAX_ERROR;
            }
        }
        RedisFuture<byte[]> future = getRedisClient().getex(key, args);
        return new FutureReply<>(future, BulkReply::bulkReply);
    }
}
