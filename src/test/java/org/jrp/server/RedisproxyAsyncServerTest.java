package org.jrp.server;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.jrp.Bootstrap;
import org.jrp.cmd.CommandProcessors;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.IllegalCommandException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;
import redis.clients.jedis.args.BitOP;
import redis.clients.jedis.args.FlushMode;
import redis.clients.jedis.args.ListDirection;
import redis.clients.jedis.args.ListPosition;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.*;
import redis.clients.jedis.resps.Slowlog;
import redis.clients.jedis.resps.Tuple;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.jrp.utils.BytesUtils.bytes;
import static org.junit.jupiter.api.Assertions.*;
import static redis.clients.jedis.params.LPosParams.lPosParams;
import static redis.clients.jedis.params.ZAddParams.zAddParams;

public class RedisproxyAsyncServerTest {

    private final Jedis redis = new Jedis("127.0.0.1", 6379);
    private final Jedis proxy = createProxyClient();

    private Jedis createProxyClient() {
        return new Jedis("127.0.0.1", 6380);
    }

    @BeforeAll
    public static void startRedisproxy() throws IllegalCommandException, IOException, InterruptedException {
        CommandProcessors.initWith(RedisServer.class.getMethods());

        ProxyConfig config = new ProxyConfig();
        config.setPort(6380);
        config.setRedisServerLoader("org.jrp.server.loader.RedisproxyAsyncServerLoader");
        Bootstrap bootstrap = new Bootstrap(config, true);
        bootstrap.start();
    }

    @Test
    public void testTime() {
        List<String> time = proxy.time();
        assertEquals(2, time.size());
        assertNotNull(time.get(0));
        assertNotNull(time.get(1));
    }

    @Test
    public void testBgrewriteaof() {
        assertEquals("Background append only file rewriting started", proxy.bgrewriteaof());
    }

    @Test
    public void testBgsave() {
        assertEquals("Background saving started", proxy.bgsave());
    }

    @Test
    public void testConfigGet() {
        assertArrayEquals(new String[]{"proxy.port", "6380"}, proxy.configGet("proxy.port").toArray());
        assertArrayEquals(new String[]{"maxmemory", "0"}, proxy.configGet("maxmemory").toArray());
    }

    @Test
    public void testConfigResetstat() {
        assertEquals("OK", proxy.configResetStat());
    }

    @Test
    public void testConfigRewrite() {
        assertEquals("OK", proxy.configRewrite());
    }

    @Test
    public void testConfigSet() {
        assertEquals("OK", proxy.configSet("proxy.timeout",
                String.valueOf(RandomUtils.nextInt(2000, 3000))));
        assertEquals("OK", proxy.configSet("proxy.maxQueuedCommands", "1024"));
        assertEquals("OK", proxy.configSet("timeout", "2000"));
    }

    @Test
    public void testFlushall() {
        assertEquals("OK", proxy.flushAll());
        assertEquals("OK", proxy.flushAll(FlushMode.ASYNC));
        assertEquals("OK", proxy.flushAll(FlushMode.SYNC));
    }

    @Test
    public void testFlushdb() {
        assertEquals("OK", proxy.flushDB());
        assertEquals("OK", proxy.flushDB(FlushMode.ASYNC));
        assertEquals("OK", proxy.flushDB(FlushMode.SYNC));
    }

    @Test
    public void testInfo() {
        assertNotNull(proxy.info());
        assertNotNull(proxy.info("Server"));
    }

    @Test
    public void testMonitor() throws InterruptedException {
        CountDownLatch monitored = new CountDownLatch(3);
        List<String> commands = new ArrayList<>();
        new Thread(() -> createProxyClient().monitor(new JedisMonitor() {
            @Override
            public void onCommand(String command) {
                commands.add(command);
                monitored.countDown();
            }
        })).start();
        TimeUnit.SECONDS.sleep(1);

        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        proxy.get(k1);
        proxy.get(k2);
        proxy.get(k3);
        assertTrue(monitored.await(1, TimeUnit.SECONDS));
        assertEquals(3, commands.size());
        assertTrue(commands.get(0).contains(k1));
        assertTrue(commands.get(1).contains(k2));
        assertTrue(commands.get(2).contains(k3));
    }

    @Test
    public void testSlowlog() {
        String slowlogThreshold = redis.configGet("slowlog-log-slower-than").get(1);
        redis.configSet("slowlog-log-slower-than", "0");
        try {
            assertTrue(proxy.slowlogLen() >= 1);

            List<Slowlog> slowlogs = proxy.slowlogGet(1);
            assertEquals(1, slowlogs.size());
            assertArrayEquals(new String[]{"SLOWLOG", "LEN"}, slowlogs.get(0).getArgs().toArray());

            assertEquals("OK", proxy.slowlogReset());
        } finally {
            redis.configSet("slowlog-log-slower-than", slowlogThreshold);
        }
    }

    String getRandomString() {
        return RandomStringUtils.randomAlphabetic(10);
    }

    @Test
    public void testAppend() {
        String key = getRandomString();
        String value = "Hello";
        assertEquals(5, proxy.append(key, value));
        assertEquals(value, redis.get(key));

        assertEquals(11, proxy.append(key, " World"));
        assertEquals("Hello World", redis.get(key));
    }

    @Test
    public void testBitcount() {
        String key = getRandomString();
        redis.set(key, "foobar");
        assertEquals(26, proxy.bitcount(key));
        assertEquals(4, proxy.bitcount(key, 0, 0));
        assertEquals(6, proxy.bitcount(key, 1, 1));
    }

    @Test
    public void testBitfield() {
        String k1 = getRandomString();
        List<Long> bitfields1 = proxy.bitfield(k1, "INCRBY", "i5", "100", "1", "GET", "u4", "0");
        assertArrayEquals(new Long[]{1L, 0L}, bitfields1.toArray());

        String k2 = getRandomString();
        List<Long> bitfields2 = proxy.bitfield(k2, "SET", "i8", "#0", "100", "SET", "i8", "#1", "100");
        assertArrayEquals(new Long[]{0L, 0L}, bitfields2.toArray());

        String k3 = getRandomString();
        List<Long> bitfield3_1 = proxy.bitfield(k3,
                "incrby", "u2", "100", "1", "OVERFLOW", "SAT", "incrby", "u2", "102", "1");
        assertArrayEquals(new Long[]{1L, 1L}, bitfield3_1.toArray());
        List<Long> bitfield3_2 = proxy.bitfield(k3,
                "incrby", "u2", "100", "1", "OVERFLOW", "SAT", "incrby", "u2", "102", "1");
        assertArrayEquals(new Long[]{2L, 2L}, bitfield3_2.toArray());
        List<Long> bitfield3_3 = proxy.bitfield(k3,
                "incrby", "u2", "100", "1", "OVERFLOW", "SAT", "incrby", "u2", "102", "1");
        assertArrayEquals(new Long[]{3L, 3L}, bitfield3_3.toArray());
        List<Long> bitfield3_4 = proxy.bitfield(k3,
                "incrby", "u2", "100", "1", "OVERFLOW", "SAT", "incrby", "u2", "102", "1");
        assertArrayEquals(new Long[]{0L, 3L}, bitfield3_4.toArray());
    }

    @Test
    public void testBitpos() {
        String k = getRandomString();
        redis.set(bytes(k), new byte[]{(byte) 0xFF, (byte) 0xF0, (byte) 0x00});
        assertEquals(12, proxy.bitpos(k, false));

        redis.set(bytes(k), new byte[]{(byte) 0x00, (byte) 0xFF, (byte) 0xF0});
        assertEquals(8, proxy.bitpos(k, true, new BitPosParams(0)));
        assertEquals(16, proxy.bitpos(k, true, new BitPosParams(2)));

        redis.set(bytes(k), new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00});
        assertEquals(-1, proxy.bitpos(k, true));
    }

    @Test
    public void testBitop() {
        String key1 = getRandomString();
        String key2 = getRandomString();
        redis.set(key1, "foobar");
        redis.set(key2, "abcdef");

        String destKey1 = getRandomString();
        assertEquals(6, proxy.bitop(BitOP.AND, destKey1, key1, key2));
        assertEquals("`bc`ab", redis.get(destKey1));

        String destKey2 = getRandomString();
        assertEquals(6, proxy.bitop(BitOP.OR, destKey2, key1, key2));
        assertEquals("goofev", redis.get(destKey2));

        String destKey3 = getRandomString();
        assertEquals(6, proxy.bitop(BitOP.XOR, destKey3, key1, key2));
        assertArrayEquals(new byte[]{7, 13, 12, 6, 4, 20}, redis.get(bytes(destKey3)));

        String destKey4 = getRandomString();
        assertEquals(6, proxy.bitop(BitOP.NOT, destKey4, key1));
        assertArrayEquals(new byte[]{-103, -112, -112, -99, -98, -115}, redis.get(bytes(destKey4)));
    }

    @Test
    public void testDecr() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.set(k1, "10");
        redis.set(k2, "234293482390480948029348230948");

        assertEquals(9, proxy.decr(k1));
        JedisDataException e = assertThrows(JedisDataException.class, () -> proxy.decr(k2));
        assertEquals("ERR value is not an integer or out of range", e.getMessage());
    }

    @Test
    public void testDecrby() {
        String k = getRandomString();
        redis.set(k, "10");
        assertEquals(7, proxy.decrBy(k, 3));
    }

    @Test
    public void testGetbit() {
        String k = getRandomString();
        redis.setbit(k, 7, true);

        assertFalse(proxy.getbit(k, 0));
        assertTrue(proxy.getbit(k, 7));
        assertFalse(proxy.getbit(k, 100));
    }

    @Test
    public void testIncrbyfloat() {
        String k = getRandomString();

        redis.set(k, "10.50");
        assertEquals(10.6, proxy.incrByFloat(k, 0.1));
        assertEquals(5.6, proxy.incrByFloat(k, -5));

        redis.set(k, "5.0e3");
        assertEquals(5200, proxy.incrByFloat(k, 2.0e2));
    }

    @Test
    public void testMget() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String nonexisting = getRandomString();
        redis.set(k1, "Hello");
        redis.set(k2, "World");

        List<String> mget = proxy.mget(k1, k2, nonexisting);
        assertEquals(3, mget.size());
        assertEquals("Hello", mget.get(0));
        assertEquals("World", mget.get(1));
        assertNull(mget.get(2));
    }

    @Test
    public void testMset() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String mset = proxy.mset(k1, "Hello", k2, "World");
        assertEquals("OK", mset);
        assertEquals("Hello", redis.get(k1));
        assertEquals("World", redis.get(k2));
    }

    @Test
    public void testMsetnx() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        assertEquals(1, proxy.msetnx(k1, "Hello", k2, "there"));
        assertEquals(0, proxy.msetnx(k2, "new", k3, "world"));
        assertArrayEquals(new String[]{"Hello", "there", null}, redis.mget(k1, k2, k3).toArray());
    }

    @Test
    public void testPsetex() {
        String k = getRandomString();
        assertEquals("OK", proxy.psetex(k, 1000, "Hello"));
        assertTrue(redis.pttl(k) > 0);
    }

    @Test
    public void testSet() {
        String k1 = getRandomString();
        assertEquals("OK", proxy.set(k1, "Hello"));
        assertEquals("Hello", redis.get(k1));

        String k2 = getRandomString();
        assertEquals("OK", proxy.set(k2, "set with ex 60", new SetParams().ex(60)));
        assertTrue(redis.ttl(k2) > 0);
        assertEquals("OK", proxy.set(k2, "set with exat +60",
                new SetParams().exAt(System.currentTimeMillis() / 1000 + 60)));
        assertTrue(redis.ttl(k2) > 0);

        String k3 = getRandomString();
        assertEquals("OK", proxy.set(k3, "set with px 60_000", new SetParams().px(60_000)));
        assertTrue(redis.ttl(k3) > 0);
        assertEquals("OK", proxy.set(k3, "set with pxat +60_000",
                new SetParams().pxAt(System.currentTimeMillis() + 60_000)));
        assertTrue(redis.ttl(k3) > 0);

        String k4 = getRandomString();
        redis.setex(k4, 60, "v");
        assertEquals("OK", proxy.set(k4, "set with keepttl", new SetParams().keepttl()));
        assertTrue(redis.ttl(k4) > 0);

        String k5 = getRandomString();
        assertEquals("OK", proxy.set(k5, "set with nx", new SetParams().nx()));
        assertNull(proxy.set(k5, "set with nx", new SetParams().nx()));

        String k6 = getRandomString();
        assertNull(proxy.set(k6, "set with xx", new SetParams().xx()));
        redis.set(k6, "v");
        assertEquals("OK", proxy.set(k6, "set with xx", new SetParams().xx()));
    }

    @Test
    public void testSetbit() {
        String k = getRandomString();
        assertFalse(proxy.setbit(k, 7, true));
        assertTrue(proxy.setbit(k, 7, false));
        assertArrayEquals(new byte[]{0}, redis.get(bytes(k)));
    }

    @Test
    public void testSetex() {
        String k = getRandomString();
        assertEquals("OK", proxy.setex(k, 10, "Hello"));
        assertTrue(redis.ttl(k) > 0);
    }

    @Test
    public void testSetnx() {
        String k = getRandomString();
        assertEquals(1, proxy.setnx(k, "Hello"));
        assertEquals(0, proxy.setnx(k, "World"));
        assertEquals("Hello", redis.get(k));
    }

    @Test
    public void testSetrange() {
        String k1 = getRandomString();
        redis.set(k1, "Hello World");
        assertEquals(11, proxy.setrange(k1, 6, "Redis"));
        assertEquals("Hello Redis", redis.get(k1));

        String k2 = getRandomString();
        assertEquals(11, proxy.setrange(k2, 6, "Redis"));
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 82, 101, 100, 105, 115}, redis.get(bytes(k2)));
    }

    @Test
    public void testStrlen() {
        String k1 = getRandomString();
        redis.set(k1, "Hello World");
        assertEquals(11, proxy.strlen(k1));

        String k2 = getRandomString();
        assertEquals(0, proxy.strlen(k2));
    }

    @Test
    public void testSubstr() {
        String k = getRandomString();
        redis.set(k, "This is a string");
        assertEquals("This", proxy.substr(k, 0, 3));
        assertEquals("ing", proxy.substr(k, -3, -1));
        assertEquals("This is a string", proxy.substr(k, 0, -1));
        assertEquals("string", proxy.substr(k, 10, 100));
    }

    @Test
    public void testLindex() {
        String k = getRandomString();
        redis.lpush(k, "World");
        redis.lpush(k, "Hello");
        assertEquals("Hello", proxy.lindex(k, 0));
        assertEquals("World", proxy.lindex(k, -1));
        assertNull(proxy.lindex(k, 3));
    }

    @Test
    public void testLinsert() {
        String k1 = getRandomString();
        redis.rpush(k1, "Hello", "World");
        assertEquals(3, proxy.linsert(k1, ListPosition.BEFORE, "World", "There"));
        assertArrayEquals(new String[]{"Hello", "There", "World"}, redis.lrange(k1, 0, -1).toArray());

        String k2 = getRandomString();
        redis.rpush(k2, "Hello", "World");
        assertEquals(3, proxy.linsert(k2, ListPosition.AFTER, "World", "There"));
        assertArrayEquals(new String[]{"Hello", "World", "There"}, redis.lrange(k2, 0, -1).toArray());
    }

    @Test
    public void testLlen() {
        String k = getRandomString();
        redis.lpush(k, "Hello", "World");
        assertEquals(2, proxy.llen(k));
    }

    @Test
    public void testLmove() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.rpush(k1, "one", "two", "three");
        assertEquals("three", proxy.lmove(k1, k2, ListDirection.RIGHT, ListDirection.LEFT));
        assertEquals("one", proxy.lmove(k1, k2, ListDirection.LEFT, ListDirection.RIGHT));
        assertEquals("three", proxy.lmove(k2, k1, ListDirection.LEFT, ListDirection.LEFT));
        assertEquals("one", proxy.lmove(k2, k1, ListDirection.RIGHT, ListDirection.RIGHT));
        assertArrayEquals(new String[]{"three", "two", "one"}, redis.lrange(k1, 0, -1).toArray());
    }

    @Test
    public void testLpop() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.rpush(k1, "one", "two", "three", "four", "five");
        assertEquals("one", proxy.lpop(k1));
        assertNull(proxy.lpop(k2));
        assertArrayEquals(new String[]{"two", "three", "four"}, proxy.lpop(k1, 3).toArray());
        assertTrue(proxy.lpop(k2, 3).isEmpty());
    }

    @Test
    public void testRpop() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.rpush(k1, "one", "two", "three", "four", "five");
        assertEquals("five", proxy.rpop(k1));
        assertNull(proxy.rpop(k2));
        assertArrayEquals(new String[]{"four", "three", "two"}, proxy.rpop(k1, 3).toArray());
        assertTrue(proxy.rpop(k2, 3).isEmpty());
    }

    @Test
    public void testLpos() {
        String k = getRandomString();
        redis.rpush(k, "a", "b", "c", "1", "2", "3", "c", "c");
        assertEquals(2, proxy.lpos(k, "c"));
        assertEquals(6, proxy.lpos(k, "c", lPosParams().rank(2)));
        assertEquals(7, proxy.lpos(k, "c", lPosParams().rank(-1)));
        assertArrayEquals(new Long[]{2L, 6L}, proxy.lpos(k, "c", lPosParams(), 2).toArray());
        assertArrayEquals(new Long[]{2L, 6L, 7L}, proxy.lpos(k, "c", lPosParams(), 0).toArray());
        assertArrayEquals(new Long[]{7L, 6L}, proxy.lpos(k, "c", lPosParams().rank(-1), 2).toArray());
        assertArrayEquals(new Long[]{2L, 6L}, proxy.lpos(k, "c", lPosParams().maxlen(7), 3).toArray());
        assertArrayEquals(new Long[]{6L},
                proxy.lpos(k, "c", lPosParams().rank(2).maxlen(7), 3).toArray());
    }

    @Test
    public void testLpush() {
        String k = getRandomString();
        assertEquals(1, proxy.lpush(k, "a"));
        assertEquals(3, proxy.lpush(k, "b", "c"));
        assertArrayEquals(new String[]{"c", "b", "a"}, redis.lrange(k, 0, -1).toArray());
    }

    @Test
    public void testRpush() {
        String k = getRandomString();
        assertEquals(1, proxy.rpush(k, "a"));
        assertEquals(3, proxy.rpush(k, "b", "c"));
        assertArrayEquals(new String[]{"a", "b", "c"}, redis.lrange(k, 0, -1).toArray());
    }

    @Test
    public void testLpushx() {
        String k1 = getRandomString();
        redis.lpush(k1, "a");
        assertEquals(2, proxy.lpushx(k1, "1"));
        assertEquals(4, proxy.lpushx(k1, "2", "3"));
        assertArrayEquals(new String[]{"3", "2", "1", "a"}, redis.lrange(k1, 0, -1).toArray());

        String k2 = getRandomString();
        assertEquals(0, proxy.lpushx(k2, "b"));
        assertTrue(redis.lrange(k2, 0, -1).isEmpty());
    }

    @Test
    public void testRpushx() {
        String k1 = getRandomString();
        redis.lpush(k1, "a");
        assertEquals(2, proxy.rpushx(k1, "1"));
        assertEquals(4, proxy.rpushx(k1, "2", "3"));
        assertArrayEquals(new String[]{"a", "1", "2", "3"}, redis.lrange(k1, 0, -1).toArray());

        String k2 = getRandomString();
        assertEquals(0, proxy.rpushx(k2, "b"));
        assertTrue(redis.lrange(k2, 0, -1).isEmpty());
    }

    @Test
    public void testLrange() {
        String k = getRandomString();
        redis.rpush(k, "one", "two", "three");
        assertArrayEquals(new String[]{"one"}, proxy.lrange(k, 0, 0).toArray());
        assertArrayEquals(new String[]{"one", "two", "three"}, proxy.lrange(k, -3, 2).toArray());
        assertArrayEquals(new String[]{"one", "two", "three"}, proxy.lrange(k, -100, 100).toArray());
        assertTrue(proxy.lrange(k, 5, 10).isEmpty());
    }

    @Test
    public void testLrem() {
        String k = getRandomString();
        redis.rpush(k, "hello", "hello", "foo", "hello");
        assertEquals(2, proxy.lrem(k, -2, "hello"));
        assertArrayEquals(new String[]{"hello", "foo"}, redis.lrange(k, 0, -1).toArray());
    }

    @Test
    public void testLset() {
        String k = getRandomString();
        redis.rpush(k, "one", "two", "three");
        assertEquals("OK", proxy.lset(k, 0, "four"));
        assertEquals("OK", proxy.lset(k, -2, "five"));
        assertArrayEquals(new String[]{"four", "five", "three"}, redis.lrange(k, 0, -1).toArray());
    }

    @Test
    public void testLtrim() {
        String k = getRandomString();
        redis.rpush(k, "one", "two", "three");
        assertEquals("OK", proxy.ltrim(k, 1, -1));
        assertArrayEquals(new String[]{"two", "three"}, redis.lrange(k, 0, -1).toArray());
    }

    @Test
    public void testRpoplpush() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.rpush(k1, "one", "two", "three");
        assertEquals("three", proxy.rpoplpush(k1, k2));
        assertArrayEquals(new String[]{"one", "two"}, redis.lrange(k1, 0, -1).toArray());
        assertArrayEquals(new String[]{"three"}, redis.lrange(k2, 0, -1).toArray());
    }

    @Test
    public void testDel() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.set(k1, "Hello");
        redis.set(k2, "World");
        assertEquals(2, proxy.del(k1, k2, k3));
    }

    @Test
    public void testExists() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.set(k1, "a");
        redis.set(k2, "b");
        assertTrue(proxy.exists(k1));
        assertEquals(2, proxy.exists(k1, k2));
        assertEquals(2, proxy.exists(k1, k2, k3));
    }

    @Test
    public void testExpire() {
        String k = getRandomString();
        redis.set(k, "Hello");
        assertEquals(1, proxy.expire(k, 10));
        assertTrue(proxy.ttl(k) > 0);
    }

    @Test
    public void testExpireat() {
        String k = getRandomString();
        redis.set(k, "Hello");
        assertEquals(1, proxy.expireAt(k, 1293840000));
        assertFalse(redis.exists(k));
    }

    @Test
    public void testHdel() {
        String k = getRandomString();
        redis.hset(k, "field1", "foo");
        assertEquals(1, proxy.hdel(k, "field1"));
        assertEquals(0, proxy.hdel(k, "field2"));
    }

    @Test
    public void testHexists() {
        String k = getRandomString();
        redis.hset(k, "field1", "foo");
        assertTrue(proxy.hexists(k, "field1"));
        assertFalse(proxy.hexists(k, "field2"));
    }

    @Test
    public void testHget() {
        String k = getRandomString();
        redis.hset(k, "field1", "foo");
        assertEquals("foo", proxy.hget(k, "field1"));
        assertNull(proxy.hget(k, "field2"));
    }

    @Test
    public void testHgetall() {
        String k = getRandomString();
        redis.hset(k, "field1", "Hello");
        redis.hset(k, "field2", "World");
        Map<String, String> hgetAll = proxy.hgetAll(k);
        assertEquals(2, hgetAll.size());
        assertEquals("Hello", hgetAll.get("field1"));
        assertEquals("World", hgetAll.get("field2"));
    }

    @Test
    public void testHincrby() {
        String k = getRandomString();
        redis.hset(k, "field", "5");
        assertEquals(6, proxy.hincrBy(k, "field", 1));
        assertEquals(5, proxy.hincrBy(k, "field", -1));
        assertEquals(-5, proxy.hincrBy(k, "field", -10));
    }

    @Test
    public void testPersist() {
        String k = getRandomString();
        redis.set(k, "Hello");
        redis.expire(k, 10);
        assertTrue(redis.ttl(k) > 0);
        assertEquals(1, proxy.persist(k));
        assertEquals(-1, redis.ttl(k));
    }

    @Test
    public void testPexpire() {
        String k = getRandomString();
        redis.set(k, "Hello");
        assertEquals(1, proxy.pexpire(k, 1500));
        assertTrue(redis.ttl(k) > 0);
    }

    @Test
    public void testPexpireat() {
        String k = getRandomString();
        redis.set(k, "Hello");
        assertEquals(1, proxy.pexpireAt(k, 1555555555005L));
        assertFalse(redis.exists(k));
    }

    @Test
    public void testPttl() {
        String k = getRandomString();
        redis.setex(k, 2, "Hello");
        assertTrue(proxy.pttl(k) > 0);
    }

    @Test
    public void testRename() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.set(k1, "Hello");
        assertEquals("OK", proxy.rename(k1, k2));
        assertEquals("Hello", redis.get(k2));
    }

    @Test
    public void testRenamenx() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.set(k1, "Hello");
        redis.set(k2, "World");
        assertEquals(0, proxy.renamenx(k1, k2));
        assertEquals("World", redis.get(k2));
    }

    @Test
    public void testSort() {
        String k1 = getRandomString();
        redis.rpush(k1, "2", "1", "4", "3");
        assertArrayEquals(new String[]{"1", "2", "3", "4"}, proxy.sort(k1).toArray());

        assertArrayEquals(new String[]{"2", "3"},
                proxy.sort(k1, new SortingParams().limit(1, 2)).toArray());

        assertArrayEquals(new String[]{"1", "2", "3", "4"}, proxy.sort(k1, new SortingParams().asc()).toArray());
        assertArrayEquals(new String[]{"4", "3", "2", "1"}, proxy.sort(k1, new SortingParams().desc()).toArray());

        String k2 = getRandomString();
        redis.rpush(k2, "b", "a", "d", "c");
        assertArrayEquals(new String[]{"a", "b", "c", "d"}, proxy.sort(k2, new SortingParams().alpha()).toArray());

        redis.mset("weight_a", "4", "weight_b", "3", "weight_c", "2", "weight_d", "1");
        assertArrayEquals(new String[]{"d", "c", "b", "a"},
                proxy.sort(k2, new SortingParams().by("weight_*")).toArray());
        assertArrayEquals(new String[]{"b", "a", "d", "c"},
                proxy.sort(k2, new SortingParams().by("nosort")).toArray());

        redis.mset("object_a", "v1", "object_b", "v2", "object_c", "v3", "object_d", "v4");
        redis.mset("value_a", "v11", "value_b", "v22", "value_c", "v33", "value_d", "v44");
        assertArrayEquals(new String[]{
                "v4", "v44", "d",
                "v3", "v33", "c",
                "v2", "v22", "b",
                "v1", "v11", "a"
        }, proxy.sort(k2, new SortingParams().by("weight_*")
                .get("object_*")
                .get("value_*")
                .get("#")).toArray());

        String k3 = getRandomString();
        String k4 = getRandomString();
        redis.rpush(k3, "-1", "-2", "-3");
        assertEquals(3, proxy.sort(k3, k4));
        assertArrayEquals(new String[]{"-3", "-2", "-1"}, redis.lrange(k4, 0, -1).toArray());
    }

    @Test
    public void testTtl() {
        String k = getRandomString();
        redis.setex(k, 2, "Hello");
        assertTrue(proxy.ttl(k) > 0);
    }

    @Test
    public void testType() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.set(k1, "Hello");
        redis.lpush(k2, "x");
        redis.sadd(k3, "x");
        assertEquals("string", proxy.type(k1));
        assertEquals("list", proxy.type(k2));
        assertEquals("set", proxy.type(k3));
    }

    @Test
    public void testSdiff() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.sadd(k1, "a", "b", "c");
        redis.sadd(k2, "c", "d", "e");
        Set<String> sdiff = proxy.sdiff(k1, k2);
        assertEquals(2, sdiff.size());
        assertTrue(sdiff.containsAll(Arrays.asList("a", "b")));
    }

    @Test
    public void testSdiffstore() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.sadd(k1, "a", "b", "c");
        redis.sadd(k2, "c", "d", "e");
        assertEquals(2, proxy.sdiffstore(k3, k1, k2));
        Set<String> smembers = redis.smembers(k3);
        assertEquals(2, smembers.size());
        assertTrue(smembers.containsAll(Arrays.asList("a", "b")));
    }

    @Test
    public void testSinter() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.sadd(k1, "a", "b", "c");
        redis.sadd(k2, "c", "d", "e");
        Set<String> sinter = proxy.sinter(k1, k2);
        assertEquals(1, sinter.size());
        assertTrue(sinter.contains("c"));
    }

    @Test
    public void testSinterstore() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.sadd(k1, "a", "b", "c");
        redis.sadd(k2, "c", "d", "e");
        assertEquals(1, proxy.sinterstore(k3, k1, k2));
        Set<String> smembers = redis.smembers(k3);
        assertEquals(1, smembers.size());
        assertTrue(smembers.contains("c"));
    }

    @Test
    public void testSismember() {
        String k = getRandomString();
        redis.sadd(k, "one");
        assertTrue(proxy.sismember(k, "one"));
        assertFalse(proxy.sismember(k, "two"));
    }

    @Test
    public void testSmembers() {
        String k = getRandomString();
        redis.sadd(k, "Hello", "World");
        Set<String> smembers = proxy.smembers(k);
        assertEquals(2, smembers.size());
        assertTrue(smembers.contains("Hello"));
        assertTrue(smembers.contains("World"));
    }

    @Test
    public void testSmove() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.sadd(k1, "one", "two");
        redis.sadd(k2, "three");
        assertEquals(1, proxy.smove(k1, k2, "two"));

        Set<String> smembers1 = proxy.smembers(k1);
        assertEquals(1, smembers1.size());
        assertTrue(smembers1.contains("one"));

        Set<String> smembers2 = proxy.smembers(k2);
        assertEquals(2, smembers2.size());
        assertTrue(smembers2.containsAll(Arrays.asList("two", "three")));
    }

    @Test
    public void testSpop() {
        String k = getRandomString();
        redis.sadd(k, "one");
        assertEquals("one", proxy.spop(k));
        redis.sadd(k, "m1", "m2");
        assertArrayEquals(new String[]{"m1", "m2"}, proxy.spop(k, 2).stream().sorted().toArray());
    }

    @Test
    public void testSrandmember() {
        String k = getRandomString();
        redis.sadd(k, "one");
        assertEquals("one", proxy.srandmember(k));
        List<String> srandmember = proxy.srandmember(k, 1);
        assertEquals(1, srandmember.size());
        assertEquals("one", srandmember.get(0));
    }

    @Test
    public void testSrem() {
        String k = getRandomString();
        redis.sadd(k, "one", "two", "three");
        assertEquals(1, proxy.srem(k, "one"));
        assertEquals(0, proxy.srem(k, "four"));
        assertArrayEquals(new String[]{"three", "two"}, redis.smembers(k).stream().sorted().toArray());
    }

    @Test
    public void testSunion() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.sadd(k1, "a", "b", "c");
        redis.sadd(k2, "c", "d", "e");
        Set<String> sunion = proxy.sunion(k1, k2);
        assertEquals(5, sunion.size());
        assertTrue(sunion.containsAll(Arrays.asList("a", "b", "c", "d", "e")));
    }

    @Test
    public void testSunionstore() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.sadd(k1, "a", "b", "c");
        redis.sadd(k2, "c", "d", "e");
        assertEquals(5, proxy.sunionstore(k3, k1, k2));
        Set<String> smembers = redis.smembers(k3);
        assertEquals(5, smembers.size());
        assertTrue(smembers.containsAll(Arrays.asList("a", "b", "c", "d", "e")));
    }

    @Test
    public void testZadd() {
        String k = getRandomString();
        assertEquals(1, proxy.zadd(k, 1, "one"));
        assertEquals(1, proxy.zadd(k, 1, "uno"));
        assertEquals(2, proxy.zadd(k, Map.of("two", 2d, "three", 3d)));
        List<Tuple> tuples = redis.zrangeWithScores(k, 0, -1);
        assertEquals(4, tuples.size());
        assertEquals(new Tuple("one", 1d), tuples.get(0));
        assertEquals(new Tuple("uno", 1d), tuples.get(1));
        assertEquals(new Tuple("two", 2d), tuples.get(2));
        assertEquals(new Tuple("three", 3d), tuples.get(3));

        assertEquals(0, proxy.zadd(k, 100d, "uno", zAddParams().xx()));
        assertEquals(0, proxy.zadd(k, 200d, "not-exist", zAddParams().xx()));
        assertEquals(4, redis.zcard(k));
        assertEquals(100, redis.zscore(k, "uno"));
        assertNull(redis.zscore(k, "not-exist"));

        assertEquals(1, proxy.zadd(k, 4d, "four", zAddParams().nx()));
        assertEquals(0, proxy.zadd(k, -1d, "one", zAddParams().nx()));
        assertEquals(5, redis.zcard(k));
        assertEquals(4, redis.zscore(k, "four"));
        assertEquals(1, redis.zscore(k, "one"));

        assertEquals(0, proxy.zadd(k, 11d, "one", zAddParams().gt()));
        assertEquals(0, proxy.zadd(k, 1d, "two", zAddParams().gt()));
        assertEquals(11, redis.zscore(k, "one"));
        assertEquals(2, redis.zscore(k, "two"));

        assertEquals(0, proxy.zadd(k, 1d, "one", zAddParams().lt()));
        assertEquals(0, proxy.zadd(k, 22d, "two", zAddParams().lt()));
        assertEquals(1, redis.zscore(k, "one"));
        assertEquals(2, redis.zscore(k, "two"));

        assertEquals(2, proxy.zadd(k,
                Map.of("five", 5d, "one", 1d, "uno", -1d), zAddParams().ch()));
    }

    @Test
    public void testZcard() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d));
        assertEquals(2, proxy.zcard(k));
    }

    @Test
    public void testZcount() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertEquals(3, proxy.zcount(k, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
        assertEquals(3, proxy.zcount(k, "-inf", "+inf"));
        assertEquals(2, proxy.zcount(k, "(1", "3"));
        assertEquals(1, proxy.zcount(k, "(1", "(3"));
    }

    @Test
    public void testZincrby() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d));
        assertEquals(3, proxy.zincrby(k, 2, "one"));
        List<Tuple> tuples = redis.zrangeWithScores(k, 0, -1);
        assertEquals(2, tuples.size());
        assertEquals(new Tuple("two", 2d), tuples.get(0));
        assertEquals(new Tuple("one", 3d), tuples.get(1));
    }

    @Test
    public void testZinterstore() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.zadd(k1, Map.of("one", 1d, "two", 2d));
        redis.zadd(k2, Map.of("one", 1d, "two", 2d, "three", 3d));
        ZParams params = new ZParams();
        params.weights(2, 3);
        assertEquals(2, proxy.zinterstore(k3, params, k1, k2));
        List<Tuple> tuples = redis.zrangeWithScores(k3, 0, -1);
        assertEquals(2, tuples.size());
        assertEquals(new Tuple("one", 5d), tuples.get(0));
        assertEquals(new Tuple("two", 10d), tuples.get(1));
    }

    @Test
    public void testZrange() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertArrayEquals(new String[]{"one", "two", "three"}, proxy.zrange(k, 0, -1).toArray());
        assertArrayEquals(new String[]{"three"}, proxy.zrange(k, 2, 3).toArray());
        assertArrayEquals(new String[]{"two", "three"}, proxy.zrange(k, -2, -1).toArray());
        assertArrayEquals(
                new Tuple[]{new Tuple("one", 1d), new Tuple("two", 2d)},
                proxy.zrangeWithScores(k, 0, 1).toArray());
    }

    @Test
    public void testZrangebyscore() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertArrayEquals(new String[]{"one", "two", "three"},
                proxy.zrangeByScore(k, "-inf", "+inf").toArray());
        assertArrayEquals(new String[]{"one", "two"}, proxy.zrangeByScore(k, 1, 2).toArray());
    }

    @Test
    public void testZrank() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertEquals(2, proxy.zrank(k, "three"));
        assertNull(proxy.zrank(k, "four"));
    }

    @Test
    public void testZrem() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertEquals(1, proxy.zrem(k, "two"));
        assertArrayEquals(
                new Tuple[]{new Tuple("one", 1d), new Tuple("three", 3d)},
                redis.zrangeWithScores(k, 0, -1).toArray());
    }

    @Test
    public void testZremrangebyrank() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertEquals(2, proxy.zremrangeByRank(k, 0, 1));
        assertArrayEquals(
                new Tuple[]{new Tuple("three", 3d)},
                redis.zrangeWithScores(k, 0, -1).toArray());
    }

    @Test
    public void testZremrangebyscore() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertEquals(2, proxy.zremrangeByScore(k, 1, 2));
        assertArrayEquals(
                new Tuple[]{new Tuple("three", 3d)},
                redis.zrangeWithScores(k, 0, -1).toArray());
    }

    @Test
    public void testZrevrange() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertArrayEquals(new String[]{"three", "two", "one"}, proxy.zrevrange(k, 0, -1).toArray());
        assertArrayEquals(new String[]{"one"}, proxy.zrevrange(k, 2, 3).toArray());
        assertArrayEquals(new String[]{"two", "one"}, proxy.zrevrange(k, -2, -1).toArray());
        assertArrayEquals(
                new Tuple[]{new Tuple("three", 3d), new Tuple("two", 2d)},
                proxy.zrevrangeWithScores(k, 0, 1).toArray());
    }

    @Test
    public void testZrevrangebyscore() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertArrayEquals(new String[]{"three", "two", "one"},
                proxy.zrevrangeByScore(k, "+inf", "-inf").toArray());
        assertArrayEquals(new String[]{"two", "one"}, proxy.zrevrangeByScore(k, 2, 1).toArray());
    }

    @Test
    public void testHincrbyfloat() {
        String k = getRandomString();
        redis.hset(k, "field", "10.50");
        assertEquals(10.6d, proxy.hincrByFloat(k, "field", 0.1));
        assertEquals(5.6d, proxy.hincrByFloat(k, "field", -5));
        redis.hset(k, "field", "5.0e3");
        assertEquals(5200, proxy.hincrByFloat(k, "field", 2.0e2));
    }

    @Test
    public void testHkeys() {
        String k = getRandomString();
        redis.hset(k, Map.of("f1", "v1", "f2", "v2"));
        Set<String> hkeys = proxy.hkeys(k);
        assertEquals(2, hkeys.size());
        assertTrue(hkeys.containsAll(Arrays.asList("f1", "f2")));
    }

    @Test
    public void testHlen() {
        String k = getRandomString();
        redis.hset(k, Map.of("f1", "v1", "f2", "v2"));
        assertEquals(2, proxy.hlen(k));
    }

    @Test
    public void testHmget() {
        String k = getRandomString();
        redis.hset(k, Map.of("f1", "v1", "f2", "v2"));
        assertArrayEquals(new String[]{"v1", "v2", null}, proxy.hmget(k, "f1", "f2", "f3").toArray());
    }

    @Test
    public void testHmset() {
        String k = getRandomString();
        assertEquals("OK", proxy.hmset(k, Map.of("f1", "v1", "f2", "v2")));
        Map<String, String> hgetAll = redis.hgetAll(k);
        assertEquals(2, hgetAll.size());
        assertEquals("v1", hgetAll.get("f1"));
        assertEquals("v2", hgetAll.get("f2"));
    }

    @Test
    public void testHset() {
        String k = getRandomString();
        assertEquals(2, proxy.hset(k, Map.of("f1", "v1", "f2", "v2")));
        Map<String, String> hgetAll = redis.hgetAll(k);
        assertEquals(2, hgetAll.size());
        assertEquals("v1", hgetAll.get("f1"));
        assertEquals("v2", hgetAll.get("f2"));
    }

    @Test
    public void testHsetnx() {
        String k = getRandomString();
        assertEquals(1, proxy.hsetnx(k, "f1", "v1"));
        assertEquals(0, proxy.hsetnx(k, "f1", "v2"));
        assertEquals("v1", redis.hget(k, "f1"));
    }

    @Test
    public void testHvals() {
        String k = getRandomString();
        redis.hset(k, Map.of("f1", "v1", "f2", "v2"));
        List<String> hvals = proxy.hvals(k);
        assertEquals(2, hvals.size());
        assertTrue(hvals.containsAll(Arrays.asList("v1", "v2")));
    }

    @Test
    public void testSadd() {
        String k = getRandomString();
        assertEquals(1, proxy.sadd(k, "m1"));
        assertEquals(1, proxy.sadd(k, "m2"));
        assertEquals(0, proxy.sadd(k, "m2"));
        Set<String> smembers = redis.smembers(k);
        assertEquals(2, smembers.size());
        assertTrue(smembers.containsAll(Arrays.asList("m1", "m2")));
    }

    @Test
    public void testScard() {
        String k = getRandomString();
        redis.sadd(k, "m1", "m2");
        assertEquals(2, proxy.scard(k));
    }

    @Test
    public void testZrevrank() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertEquals(2, proxy.zrevrank(k, "one"));
        assertNull(proxy.zrank(k, "four"));
    }

    @Test
    public void testZscore() {
        String k = getRandomString();
        redis.zadd(k, Map.of("one", 1d, "two", 2d, "three", 3d));
        assertEquals(1, proxy.zscore(k, "one"));
    }

    @Test
    public void testZunionstore() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.zadd(k1, Map.of("one", 1d, "two", 2d));
        redis.zadd(k2, Map.of("one", 1d, "two", 2d, "three", 3d));
        ZParams params = new ZParams();
        params.weights(2, 3);
        assertEquals(3, proxy.zunionstore(k3, params, k1, k2));
        List<Tuple> tuples = redis.zrangeWithScores(k3, 0, -1);
        assertEquals(3, tuples.size());
        assertEquals(new Tuple("one", 5d), tuples.get(0));
        assertEquals(new Tuple("three", 9d), tuples.get(1));
        assertEquals(new Tuple("two", 10d), tuples.get(2));
    }

    @Test
    public void testGetrange() {
        String k = getRandomString();
        redis.set(k, "This is a string");
        assertEquals("This", proxy.getrange(k, 0, 3));
        assertEquals("ing", proxy.getrange(k, -3, -1));
        assertEquals("This is a string", proxy.getrange(k, 0, -1));
        assertEquals("string", proxy.getrange(k, 10, 100));
    }

    @Test
    public void testGetset() {
        String k = getRandomString();
        redis.set(k, "Hello");
        assertEquals("Hello", proxy.getSet(k, "World"));
        assertEquals("World", redis.get(k));
    }

    @Test
    public void testIncr() {
        String k = getRandomString();
        redis.set(k, "10");
        assertEquals(11, proxy.incr(k));
        assertEquals("11", redis.get(k));
    }

    @Test
    public void testIncrby() {
        String k = getRandomString();
        redis.set(k, "10");
        assertEquals(15, proxy.incrBy(k, 5));
    }

    @Test
    public void testPfadd() {
        String k = getRandomString();
        assertEquals(1, proxy.pfadd(k, "a", "b", "c", "d", "e", "f", "g"));
        assertEquals(7, redis.pfcount(k));
    }

    @Test
    public void testPfcount() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        redis.pfadd(k1, "foo", "bar", "zap");
        redis.pfadd(k1, "zap", "zap", "zap");
        redis.pfadd(k1, "foo", "bar");
        assertEquals(3, proxy.pfcount(k1));
        redis.pfadd(k2, "1", "2", "3");
        assertEquals(6, proxy.pfcount(k1, k2));
    }

    @Test
    public void testPfmerge() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        String k3 = getRandomString();
        redis.pfadd(k1, "foo", "bar", "zap", "a");
        redis.pfadd(k2, "a", "b", "c", "foo");
        redis.pfadd(k1, "foo", "bar");
        assertEquals("OK", proxy.pfmerge(k3, k1, k2));
        assertEquals(6, proxy.pfcount(k3));
    }

    @Test
    public void testGet() {
        String k1 = getRandomString();
        String k2 = getRandomString();
        assertNull(proxy.get(k1));
        redis.set(k2, "v");
        assertEquals("v", proxy.get(k2));
    }

    @Test
    public void testGetdel() {
        String k = getRandomString();
        assertNull(proxy.getDel(k));
        redis.set(k, "v");
        assertEquals("v", proxy.getDel(k));
        assertNull(redis.get(k));
    }

    @Test
    public void testGetex() {
        String k = getRandomString();
        redis.set(k, "v");
        assertEquals("v", proxy.getEx(k, new GetExParams()));
        assertEquals(-1, redis.ttl(k));
        assertEquals("v", proxy.getEx(k, new GetExParams().ex(60)));
        assertTrue(redis.ttl(k) > 0);
        assertEquals("v", proxy.getEx(k, new GetExParams().px(60_000)));
        assertTrue(redis.ttl(k) > 0);
        assertEquals("v", proxy.getEx(k, new GetExParams().exAt(System.currentTimeMillis() / 1000 + 60)));
        assertTrue(redis.ttl(k) > 0);
        assertEquals("v", proxy.getEx(k, new GetExParams().pxAt(System.currentTimeMillis() + 60_000)));
        assertTrue(redis.ttl(k) > 0);
        assertEquals("v", proxy.getEx(k, new GetExParams().persist()));
        assertEquals(-1, redis.ttl(k));
    }
}
