package org.jrp.cmd;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jrp.exception.IllegalCommandException;
import org.jrp.exception.RedisException;
import org.jrp.reply.BulkReply;
import org.jrp.reply.Reply;
import org.jrp.server.AbstractRedisServer;
import org.jrp.server.RedisServer;
import org.jrp.utils.BytesUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.jrp.cmd.RWType.Type.*;
import static org.jrp.utils.BytesUtils.bytes;
import static org.junit.jupiter.api.Assertions.*;

public class CommandProcessorTest {

    interface MyRedisServer extends RedisServer {

        @RWType(type = READ)
        void readMethod();

        @RWType(type = WRITE)
        void writeMethod();

        @RWType(type = OTHER)
        void otherMethod1();

        @RWType
        void otherMethod2();

        void otherMethod3();

        @Override
        String toString();
    }

    @Test
    public void testGetRWType() throws NoSuchMethodException {
        Method readMethod = MyRedisServer.class.getDeclaredMethod("readMethod");
        Method writeMethod = MyRedisServer.class.getDeclaredMethod("writeMethod");
        Method otherMethod1 = MyRedisServer.class.getDeclaredMethod("otherMethod1");
        Method otherMethod2 = MyRedisServer.class.getDeclaredMethod("otherMethod2");
        Method otherMethod3 = MyRedisServer.class.getDeclaredMethod("otherMethod3");
        Method toStringMethod = MyRedisServer.class.getDeclaredMethod("toString");

        assertEquals(READ, CommandProcessor.getRWType(readMethod));
        assertEquals(WRITE, CommandProcessor.getRWType(writeMethod));
        assertEquals(OTHER, CommandProcessor.getRWType(otherMethod1));
        assertEquals(OTHER, CommandProcessor.getRWType(otherMethod2));
        assertEquals(OTHER, CommandProcessor.getRWType(otherMethod3));
        assertEquals(OTHER, CommandProcessor.getRWType(toStringMethod));
    }

    @Test
    public void testExecute() throws NoSuchMethodException, IllegalCommandException {
        RedisServer myRedisServer = new AbstractRedisServer(null) {
            @Override
            public Reply get(byte[] key) {
                return BulkReply.bulkReply("GET key " + BytesUtils.string(key) + " from myRedisServer");
            }
        };

        String randomString = RandomStringUtils.randomAscii(10);
        Command getCommand = new Command(new byte[][]{bytes("GET"), bytes(randomString)});

        Method getMethod = myRedisServer.getClass().getDeclaredMethod("get", byte[].class);
        CommandProcessor getProcessor = new CommandProcessor("GET", getMethod);

        Reply reply;
        try {
            reply = getProcessor.execute(getCommand, myRedisServer);
        } catch (RedisException e) {
            fail("not expected exception thrown: " + e);
            return;
        }
        assertTrue(reply instanceof BulkReply);
        assertEquals("GET key " + randomString + " from myRedisServer", reply.toString());
    }

    @Test
    public void testExecuteWithException() throws NoSuchMethodException, IllegalCommandException {
        RedisServer myRedisServer = new AbstractRedisServer(null) {
            @Override
            public Reply get(byte[] key) {
                throw new RuntimeException("expected");
            }
        };

        String randomString = RandomStringUtils.randomAscii(10);
        Command getCommand = new Command(new byte[][]{bytes("GET"), bytes(randomString)});

        Method getMethod = myRedisServer.getClass().getDeclaredMethod("get", byte[].class);
        CommandProcessor getProcessor = new CommandProcessor("GET", getMethod);

        RedisException ex = assertThrows(RedisException.class,
                () -> getProcessor.execute(getCommand, myRedisServer));
        assertEquals("expected", ExceptionUtils.getRootCause(ex).getMessage());
    }

    @Test
    public void testCheckValid() throws NoSuchMethodException {
        //noinspection unused
        RedisServer myRedisServer = new AbstractRedisServer(null) {
            public Reply foo(byte[] key, byte[][] values) {
                return null;
            }
        };

        Method fooCommand = myRedisServer.getClass().getDeclaredMethod("foo", byte[].class, byte[][].class);
        try {
            new CommandProcessor("FOO", fooCommand);
        } catch (IllegalCommandException e) {
            fail("not expected exception thrown: " + e);
        }
    }

    @Test
    public void testCheckValidForIllegalCommandWithWrongParameterPosition() throws NoSuchMethodException {
        //noinspection unused
        RedisServer myRedisServer = new AbstractRedisServer(null) {
            public Reply foo(byte[] key, byte[][] values, byte[] option) {
                return null;
            }
        };

        Method fooCommand = myRedisServer.getClass().getDeclaredMethod(
                "foo", byte[].class, byte[][].class, byte[].class);
        IllegalCommandException ex = assertThrows(IllegalCommandException.class,
                () -> new CommandProcessor("FOO", fooCommand));
        assertEquals(
                "Error for method foo : byte[][] parameter type can only be put at last!",
                ex.getMessage());
    }

    @Test
    public void testCheckValidForIllegalCommandWithWrongParameterType() throws NoSuchMethodException {
        //noinspection unused
        RedisServer myRedisServer = new AbstractRedisServer(null) {
            public Reply foo(byte[] key, String value) {
                return null;
            }
        };

        Method fooCommand = myRedisServer.getClass().getDeclaredMethod("foo", byte[].class, String.class);
        IllegalCommandException ex = assertThrows(IllegalCommandException.class,
                () -> new CommandProcessor("FOO", fooCommand));
        assertEquals(
                "Error for method foo : only byte[] or byte[][] parameter types are allowed!",
                ex.getMessage());
    }
}
