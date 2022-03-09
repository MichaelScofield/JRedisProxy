package org.jrp.cmd;

import org.apache.commons.lang3.RandomStringUtils;
import org.jrp.exception.IllegalCommandException;
import org.jrp.server.RedisServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

import static org.jrp.utils.BytesUtils.bytes;
import static org.jrp.utils.BytesUtils.string;
import static org.junit.jupiter.api.Assertions.*;

public class CommandProcessorsTest {

    // 'AfterEach' will be called even some tests throw exceptions.
    @AfterEach
    public void restoreRedisServerCommandProcessors() throws IllegalCommandException {
        CommandProcessors.initWith(RedisServer.class.getMethods());
    }

    @Test
    public void testInitWith() throws IllegalCommandException {
        //noinspection unused
        interface MyRedisServer {

            byte[] get(byte[] key);

            byte[] set(byte[] key, byte[] value);

            byte[] setex(byte[] key, byte[] value);

            byte[] setnx(byte[] key, byte[] value);

            byte[] zadd(byte[] key, byte[][] others);

            byte[] zrange(byte[] key, byte[][] others);

            byte[] zrangeWithScores(byte[] key, byte[][] others);

            byte[] zrangeWithScoresByScores(byte[] key, byte[][] others);
        }

        CommandProcessors.initWith(MyRedisServer.class.getMethods());
        CommandProcessor[][] processors = CommandProcessors.getCommandProcessors();
        for (int i = 0; i < 26; i++) {
            switch ((char) ('A' + i)) {
                case 'G':
                case 'S':
                case 'Z':
                    continue;
                default:
                    assertNull(processors[i]);
            }
        }

        CommandProcessor[] getProcessors = processors['G' - 'A'];
        assertEquals(1, getProcessors.length);
        CommandProcessor getProcessor = getProcessors[0];
        assertEquals("get", getProcessor.getCommandMethod().getName());

        CommandProcessor[] setProcessors = processors['S' - 'A'];
        Arrays.sort(setProcessors, Comparator.comparing(CommandProcessor::getCommandName));
        assertEquals(3, setProcessors.length);
        CommandProcessor setProcessor = setProcessors[0];
        assertEquals("set", setProcessor.getCommandMethod().getName());
        CommandProcessor setexProcessor = setProcessors[1];
        assertEquals("setex", setexProcessor.getCommandMethod().getName());
        CommandProcessor setnxProcessor = setProcessors[2];
        assertEquals("setnx", setnxProcessor.getCommandMethod().getName());

        CommandProcessor[] zsetProcessors = processors['Z' - 'A'];
        Arrays.sort(zsetProcessors, Comparator.comparing(CommandProcessor::getCommandName));
        assertEquals(4, zsetProcessors.length);
        CommandProcessor zaddProcessor = zsetProcessors[0];
        assertEquals("zadd", zaddProcessor.getCommandMethod().getName());
        CommandProcessor zrangeProcessor = zsetProcessors[1];
        assertEquals("zrange", zrangeProcessor.getCommandMethod().getName());
        CommandProcessor zrangeWithScoresProcessor = zsetProcessors[2];
        assertEquals("zrangeWithScores", zrangeWithScoresProcessor.getCommandMethod().getName());
        CommandProcessor zrangeWithScoresByScoresProcessor = zsetProcessors[3];
        assertEquals("zrangeWithScoresByScores",
                zrangeWithScoresByScoresProcessor.getCommandMethod().getName());
    }

    @Test
    public void testGetCommandProcessor() {
        byte[] command1 = "GET".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(command1, CommandProcessors.get(command1).getCommandNameBytes());

        byte[] command2 = "get".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(command2, CommandProcessors.get(command2).getCommandNameBytes());

        byte[] command3 = "YY".getBytes(StandardCharsets.UTF_8);
        assertNull(CommandProcessors.get(command3));

        byte[] command4 = "A".getBytes(StandardCharsets.UTF_8);
        assertNull(CommandProcessors.get(command4));

        assertNull(CommandProcessors.get(null));
    }

    @Test
    public void testAddCommandProcessor() throws IllegalCommandException {
        CommandProcessor getProcessor = CommandProcessors.get(bytes("get"));
        CommandProcessors.add(new CommandProcessor("TEG", getProcessor.getCommandMethod()));
        CommandProcessor tegProcessor = CommandProcessors.get(bytes("teg"));
        assertEquals("TEG", tegProcessor.getCommandName());
    }

    @Test
    public void testFailedToAddExistingCommandProcessor() {
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            CommandProcessor setProcessor = CommandProcessors.get(bytes("set"));
            CommandProcessors.add(new CommandProcessor("set", setProcessor.getCommandMethod()));
        });
        assertEquals("unable to add exist commandProcessor set", exception.getMessage());
    }

    @Test
    public void testRemoveCommandProcessor() throws IllegalCommandException {
        CommandProcessor getProcessor = CommandProcessors.get(bytes("get"));
        byte[] newCommand = bytes(RandomStringUtils.randomAlphabetic(4));
        CommandProcessors.add(new CommandProcessor(string(newCommand), getProcessor.getCommandMethod()));

        CommandProcessors.remove(newCommand);
        assertNull(CommandProcessors.get(newCommand));
    }

    @Test
    public void testFailedToRemoveNotExistCommandProcessor() {
        String newCommand = RandomStringUtils.randomAlphabetic(4);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> CommandProcessors.remove(bytes(newCommand)));
        assertEquals(
                "unable to remove not exist commandProcessor " + newCommand.toUpperCase(),
                ex.getMessage());
    }
}
