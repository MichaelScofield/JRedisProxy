package org.jrp.server.handler;

import com.google.common.collect.ImmutableMap;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.CommandType;
import io.netty.channel.DefaultChannelId;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.jrp.cmd.Command;
import org.jrp.cmd.CommandLifecycle;
import org.jrp.cmd.InvalidCommand;
import org.jrp.config.ProxyConfig;
import org.jrp.exception.IllegalCommandException;
import org.jrp.exception.RedisException;
import org.jrp.monitor.ClientStat;
import org.jrp.monitor.CommandTimeMonitor;
import org.jrp.monitor.metrics.CmdLifecycleMetrics;
import org.jrp.monitor.metrics.RedisproxyMetrics;
import org.jrp.monitor.metrics.RedisproxyStatusMetrics;
import org.jrp.reply.*;
import org.jrp.server.AbstractRedisServer;
import org.jrp.server.RedisServerContext;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.jrp.utils.BytesUtils.bytes;
import static org.jrp.utils.BytesUtils.string;
import static org.junit.jupiter.api.Assertions.*;

public class RedisCommandHandlerTest {

    @Test
    public void testRenamedCommands() throws IllegalCommandException {
        TestRedisServer redisServer = new TestRedisServer();
        ProxyConfig proxyConfig = new ProxyConfig();
        proxyConfig.setRenameCommands(ImmutableMap.of("getset", "what", "strlen", ""));
        RedisCommandHandler handler = new RedisCommandHandler(redisServer, proxyConfig);

        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        String key = RandomStringUtils.randomAlphabetic(10);
        String value = RandomStringUtils.randomAlphabetic(10);

        Command whatCommand = new Command(new byte[][]{bytes("what"), bytes(key), bytes(value)});
        channel.writeInbound(whatCommand);
        channel.flush();
        Reply reply1 = channel.readOutbound();
        assertEquals("null", reply1.toString());
        assertEquals(value, redisServer.data.get(key));

        Command getsetCommand = new Command(new byte[][]{bytes("getset"), bytes(key), bytes(value)});
        channel.writeInbound(getsetCommand);
        channel.flush();
        Reply reply2 = channel.readOutbound();
        assertEquals(String.format("unknown command \"GETSET\" \"%s\" \"%s\"", key, value), reply2.toString());

        Command strlenCommand = new Command(new byte[][]{bytes("strlen"), bytes(key)});
        channel.writeInbound(strlenCommand);
        channel.flush();
        Reply reply3 = channel.readOutbound();
        assertEquals(String.format("unknown command \"STRLEN\" \"%s\"", key), reply3.toString());
    }

    @Test
    public void testChannelRead() throws IllegalCommandException {
        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        String key = RandomStringUtils.randomAlphabetic(10);
        String value = RandomStringUtils.randomAlphabetic(10);

        long procReadBefore = RedisproxyMetrics.getCurrent().procRead.get();
        long procWriteBefore = RedisproxyMetrics.getCurrent().procWrite.get();
        long procOtherBefore = RedisproxyMetrics.getCurrent().procOther.get();

        long queuedBefore = RedisproxyStatusMetrics.INSTANCE.queued.get();
        long threadsBefore = RedisproxyStatusMetrics.INSTANCE.threads.get();

        Command setCommand = new Command(new byte[][]{bytes("SET"), bytes(key), bytes(value)});
        Command getCommand = new Command(new byte[][]{bytes("GET"), bytes(key)});
        Command dbsizeCommand = new Command(new byte[][]{bytes("DBSIZE")});

        channel.writeInbound(setCommand);
        channel.flush();
        Reply reply1 = channel.readOutbound();
        assertEquals("OK", reply1.toString());

        channel.writeInbound(getCommand);
        channel.flush();
        Reply reply2 = channel.readOutbound();
        assertEquals(value, reply2.toString());

        channel.writeInbound(dbsizeCommand);
        channel.flush();
        Reply reply3 = channel.readOutbound();
        assertEquals("1", reply3.toString());

        assertEquals("embedded", setCommand.getClientAddress());
        assertEquals("embedded", getCommand.getClientAddress());
        assertEquals("embedded", dbsizeCommand.getClientAddress());

        long procReadAfter = RedisproxyMetrics.getCurrent().procRead.get();
        long procWriteAfter = RedisproxyMetrics.getCurrent().procWrite.get();
        long procOtherAfter = RedisproxyMetrics.getCurrent().procOther.get();
        assertEquals(procReadBefore + 1, procReadAfter);
        assertEquals(procWriteBefore + 1, procWriteAfter);
        assertEquals(procOtherBefore + 1, procOtherAfter);

        long queuedAfter = RedisproxyStatusMetrics.INSTANCE.queued.get();
        long threadsAfter = RedisproxyStatusMetrics.INSTANCE.threads.get();
        assertEquals(queuedBefore - 3, queuedAfter);
        assertEquals(threadsBefore, threadsAfter);

        assertNull(RedisServerContext.getChannel());
        assertNull(RedisServerContext.getCommand());

        assertTrue(channel.isActive());
    }

    @Test
    public void testHandleCommandWithProxyTimeout() throws IllegalCommandException, InterruptedException {
        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance(), handler);
        ClientStat.active(channel);

        ClientStat stat = ClientStat.getStat(channel);
        stat.setProxyTimeout(1);

        Command command = new Command(new byte[][]{bytes("GET"), bytes("foo")});
        TimeUnit.SECONDS.sleep(1);
        long discardedBefore = RedisproxyMetrics.getCurrent().discarded.get();

        channel.writeInbound(command);
        channel.flush();
        Reply reply = channel.readOutbound();
        assertNull(reply);

        long discardedAfter = RedisproxyMetrics.getCurrent().discarded.get();
        assertEquals(discardedBefore + 1, discardedAfter);
    }

    @Test
    public void testHandleInvalidCommand() throws IllegalCommandException {
        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        InvalidCommand command = new InvalidCommand(new Exception("expected"));
        channel.writeInbound(command);
        channel.flush();
        Reply reply = channel.readOutbound();
        assertEquals("Exception: expected", reply.toString());
        assertFalse(channel.isActive());
    }

    @Test
    public void testExecuteCommandInReadonlyRedisServer() throws IllegalCommandException {
        ProxyConfig config = new ProxyConfig();
        config.setReadOnly(true);

        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), config);
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        Command setCommand = new Command(new byte[][]{bytes("SET"), bytes("foo"), bytes("bar")});
        channel.writeInbound(setCommand);
        channel.flush();
        Reply reply = channel.readOutbound();
        assertEquals("Readonly", reply.toString());
    }

    @Test
    public void testExecuteCommandWithException() throws IllegalCommandException {
        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        String s1 = RandomStringUtils.randomAlphabetic(10);
        String s2 = RandomStringUtils.randomAlphabetic(10);
        Command command = new Command(new byte[][]{bytes("APPEND"), bytes(s1), bytes(s2)});
        channel.writeInbound(command);
        channel.flush();
        Reply reply = channel.readOutbound();
        assertEquals(String.format(
                "unable to handle command \"APPEND\" \"%s\" \"%s\", error: RedisException: expected",
                s1, s2), reply.toString());
    }

    @Test
    public void testExecuteCommandWithNullReply() throws IllegalCommandException {
        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        Command command = new Command(new byte[][]{bytes("DECR"), bytes("foo")});
        channel.writeInbound(command);
        channel.flush();
        Reply reply = channel.readOutbound();
        assertEquals("nil", reply.toString());
    }

    @Test
    public void testHandleAsyncReply() throws IllegalCommandException, InterruptedException {
        CountDownLatch asyncReplyLatch = new CountDownLatch(1);
        TestRedisServer redisServer = new TestRedisServer();
        redisServer.asyncExecLatch = asyncReplyLatch;

        RedisCommandHandler handler = new RedisCommandHandler(redisServer, new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);


        Command ttlCommand = new Command(new byte[][]{bytes("TTL"), bytes("foo")});
        channel.writeInbound(ttlCommand);
        channel.flush();
        assertTrue(asyncReplyLatch.await(1, TimeUnit.SECONDS));

        // make sure reply is written
        TimeUnit.MILLISECONDS.sleep(100);

        Reply reply1 = channel.readOutbound();
        assertTrue(reply1 instanceof AsyncReply);
        assertEquals("1", ((AsyncReply<?>) reply1).getReply().toString());


        Command pttlCommand = new Command(new byte[][]{bytes("PTTL"), bytes("foo")});
        channel.writeInbound(pttlCommand);
        channel.flush();

        // make sure reply is written
        TimeUnit.MILLISECONDS.sleep(100);

        Reply reply2 = channel.readOutbound();
        assertTrue(reply2 instanceof ErrorReply);
        assertEquals(
                "unable to handle command \"PTTL\" \"foo\", error: RuntimeException: expected",
                reply2.toString());
    }

    @Test
    public void testHandleFutureReply() throws IllegalCommandException {
        AsyncCommand<String, String, String> asyncCommand = new AsyncCommand<>(
                new io.lettuce.core.protocol.Command<>(CommandType.INCR, new ValueOutput<>(new StringCodec())));
        TestRedisServer redisServer = new TestRedisServer();
        redisServer.asyncCommand = asyncCommand;

        RedisCommandHandler handler = new RedisCommandHandler(redisServer, new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        Command command = new Command(new byte[][]{bytes("INCR"), bytes("foo")});
        channel.writeInbound(command);
        channel.flush();

        String s = RandomStringUtils.randomAlphabetic(10);
        asyncCommand.complete(s);
        Reply reply = channel.readOutbound();
        assertEquals(s, reply.toString());
    }

    @Test
    public void testRecordCommandExecution() throws IllegalCommandException, InterruptedException {
        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        Command command = new Command(new byte[][]{bytes("GET"), bytes("foo")});
        long slowExecBefore = RedisproxyMetrics.getCurrent().slowExec.get();
        TimeUnit.MILLISECONDS.sleep(11);

        channel.writeInbound(command);
        channel.flush();
        Reply reply = channel.readOutbound();
        assertEquals("bar", reply.toString());

        assertEquals(CommandLifecycle.STATE.FINALIZE, command.getCommandLifecycle().getState());
        long slowExecAfter = RedisproxyMetrics.getCurrent().slowExec.get();
        assertEquals(slowExecBefore + 1, slowExecAfter);
        assertTrue(CommandTimeMonitor.getCurrent().getStat().contains("GET"));
        assertTrue(StringUtils.isNotBlank(CmdLifecycleMetrics.getCurrent().getStat()));
    }

    @Test
    public void testHandleSelectCommand() throws IllegalCommandException {
        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(DefaultChannelId.newInstance(), handler);
        ClientStat.active(channel);

        Command command = new Command(new byte[][]{bytes("SELECT"), bytes("42")});
        channel.writeInbound(command);
        channel.flush();

        Reply reply = channel.readOutbound();
        assertEquals("OK", reply.toString());
        ClientStat stat = ClientStat.getStat(channel);
        assertEquals(42, stat.getDb());
    }

    @Test
    public void testHandleQuitCommand() throws IllegalCommandException {
        RedisCommandHandler handler = new RedisCommandHandler(new TestRedisServer(), new ProxyConfig());
        EmbeddedChannel channel = new EmbeddedChannel(handler);
        ClientStat.active(channel);

        Command command = new Command(new byte[][]{bytes("QUIT")});
        channel.writeInbound(command);
        channel.flush();

        Reply reply = channel.readOutbound();
        assertEquals("OK", reply.toString());
        assertFalse(channel.isActive());
    }

    public static class TestRedisServer extends AbstractRedisServer {

        private final Map<String, String> data = new HashMap<>();
        CountDownLatch asyncExecLatch;
        AsyncCommand<String, String, String> asyncCommand;

        public TestRedisServer() {
            super(null);
            data.put("foo", "bar");
        }

        @Override
        public Reply get(byte[] key) {
            return BulkReply.bulkReply(data.get(string(key)));
        }

        @Override
        public Reply set(byte[] rawKey, byte[] rawVal, byte[][] options) {
            data.put(string(rawKey), string(rawVal));
            return SimpleStringReply.OK;
        }

        @Override
        public Reply getset(byte[] rawKey, byte[] rawVal) {
            String key = string(rawKey);
            String value = string(rawVal);
            String reply = data.get(key);
            data.put(key, value);
            return BulkReply.bulkReply(reply);
        }

        @Override
        public Reply strlen(byte[] key) {
            return IntegerReply.integer(data.get(string(key)).length());
        }

        @Override
        public Reply dbsize() {
            return IntegerReply.integer(1);
        }

        @Override
        public Reply append(byte[] key, byte[] value) {
            throw new RedisException("expected");
        }

        @Override
        public Reply incr(byte[] key) {
            return new FutureReply<>(asyncCommand, BulkReply::bulkReply);
        }

        @Override
        public Reply decr(byte[] key) {
            return null;
        }

        @Override
        public Reply ttl(byte[] key) {
            return new AsyncReply<>(() -> {
                asyncExecLatch.countDown();
                return IntegerReply.integer(1);
            });
        }

        @Override
        public Reply pttl(byte[] key) {
            return new AsyncReply<>(() -> {
                throw new RuntimeException("expected");
            });
        }
    }
}
