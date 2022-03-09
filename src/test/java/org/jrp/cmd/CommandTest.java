package org.jrp.cmd;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.jrp.utils.BytesUtils;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Stream;

import static org.jrp.cmd.Command.MAX_PRINTED_ARGS;
import static org.jrp.cmd.Command.MAX_PRINTED_TOKEN_LENGTH;
import static org.jrp.utils.BytesUtils.bytes;
import static org.jrp.utils.BytesUtils.string;
import static org.junit.jupiter.api.Assertions.*;

public class CommandTest {

    public interface RedisMethods {

        byte[] zeroArityMethod();

        byte[] oneArityMethod(byte[] key);

        byte[] twoArityMethod(byte[] key, byte[] value);

        byte[] twoArityMethodWithVarargs(byte[] key, byte[][] values);

        byte[] manyArityMethod(byte[] key, byte[] value, byte[] option);

        byte[] manyArityMethodWithVarargs(byte[] key, byte[] value, byte[][] options);

        byte[] methodWithVarargsNotAtLast(byte[] key, byte[][] values, byte[] wouldBeNull);

        byte[] methodWithMoreArgs(byte[] key, byte[] value, byte[] option);

        byte[] methodWithLessArgs(byte[] key, byte[] value);

        byte[] methodWithOtherTypesThanBytesArray(byte[] key, String value);
    }

    @Test
    public void testNewCommand() {
        Command cmd1 = new Command();
        assertTrue(cmd1.id > 0);
        assertNull(cmd1.getTokens());
        assertNull(cmd1.getCommandProcessor());

        long randomId = RandomUtils.nextLong();
        Command cmd2 = new Command(randomId);
        assertEquals(randomId, cmd2.id);
        assertNull(cmd2.getTokens());
        assertNull(cmd2.getCommandProcessor());

        byte[][] tokens = {bytes("GET"), bytes(RandomStringUtils.randomAscii(10))};
        Command cmd3 = new Command(tokens);
        assertTrue(cmd3.id > 0);
        assertArrayEquals(tokens, cmd3.getTokens());
        assertSame(CommandProcessors.get(bytes("GET")), cmd3.getCommandProcessor());
    }

    @Test
    public void testToArgumentsForZeroArityMethod() throws NoSuchMethodException {
        Command cmd = new Command(new byte[][]{bytes("TIME")});
        Method zeroArityMethod = RedisMethods.class.getDeclaredMethod("zeroArityMethod");
        Object[] arguments = cmd.toArguments(zeroArityMethod.getParameterTypes());
        assertEquals(0, arguments.length);
    }

    @Test
    public void testToArgumentsForOneArityMethod() throws NoSuchMethodException {
        String randomString = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("GET"), bytes(randomString)});

        Method oneArityMethod = RedisMethods.class.getDeclaredMethod("oneArityMethod", byte[].class);
        Object[] arguments = cmd.toArguments(oneArityMethod.getParameterTypes());
        assertEquals(1, arguments.length);

        assertTrue(arguments[0] instanceof byte[]);
        assertEquals(randomString, string((byte[]) arguments[0]));
    }

    @Test
    public void testToArgumentsForTwoArityMethod() throws NoSuchMethodException {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"), bytes(randomString1), bytes(randomString2)});

        Method twoArityMethod = RedisMethods.class
                .getDeclaredMethod("twoArityMethod", byte[].class, byte[].class);
        Object[] arguments = cmd.toArguments(twoArityMethod.getParameterTypes());
        assertEquals(2, arguments.length);

        assertEquals(randomString1, string((byte[]) arguments[0]));
        assertEquals(randomString2, string((byte[]) arguments[1]));
    }

    @Test
    public void testToArgumentsForTwoArityMethodWithVarargs() throws NoSuchMethodException {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        String randomString3 = RandomStringUtils.randomAscii(10);
        String randomString4 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"),
                bytes(randomString1), bytes(randomString2), bytes(randomString3), bytes(randomString4)});

        Method twoArityMethodWithVarargs = RedisMethods.class
                .getDeclaredMethod("twoArityMethodWithVarargs", byte[].class, byte[][].class);
        Object[] arguments = cmd.toArguments(twoArityMethodWithVarargs.getParameterTypes());
        assertEquals(2, arguments.length);

        assertEquals(randomString1, string((byte[]) arguments[0]));

        byte[][] varargs = (byte[][]) arguments[1];
        assertEquals(randomString2, string(varargs[0]));
        assertEquals(randomString3, string(varargs[1]));
        assertEquals(randomString4, string(varargs[2]));
    }

    @Test
    public void testToArgumentsForManyArityMethod() throws NoSuchMethodException {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        String randomString3 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"),
                bytes(randomString1), bytes(randomString2), bytes(randomString3)});

        Method manyArityMethod = RedisMethods.class
                .getDeclaredMethod("manyArityMethod", byte[].class, byte[].class, byte[].class);
        Object[] arguments = cmd.toArguments(manyArityMethod.getParameterTypes());
        assertEquals(3, arguments.length);

        assertEquals(randomString1, string((byte[]) arguments[0]));
        assertEquals(randomString2, string((byte[]) arguments[1]));
        assertEquals(randomString3, string((byte[]) arguments[2]));
    }

    @Test
    public void testToArgumentsForManyArityMethodWithVarargs() throws NoSuchMethodException {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        String randomString3 = RandomStringUtils.randomAscii(10);
        String randomString4 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"),
                bytes(randomString1), bytes(randomString2), bytes(randomString3), bytes(randomString4)});

        Method manyArityMethodWithVarargs = RedisMethods.class
                .getDeclaredMethod("manyArityMethodWithVarargs", byte[].class, byte[].class, byte[][].class);
        Object[] arguments = cmd.toArguments(manyArityMethodWithVarargs.getParameterTypes());
        assertEquals(3, arguments.length);

        assertEquals(randomString1, string((byte[]) arguments[0]));
        assertEquals(randomString2, string((byte[]) arguments[1]));

        byte[][] varargs = (byte[][]) arguments[2];
        assertEquals(randomString3, string(varargs[0]));
        assertEquals(randomString4, string(varargs[1]));
    }

    @Test
    public void testToArgumentsForMethodWithVarargsNotAtLast() throws NoSuchMethodException {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        String randomString3 = RandomStringUtils.randomAscii(10);
        String randomString4 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"),
                bytes(randomString1), bytes(randomString2), bytes(randomString3), bytes(randomString4)});

        Method methodWithVarargsNotAtLast = RedisMethods.class
                .getDeclaredMethod("methodWithVarargsNotAtLast", byte[].class, byte[][].class, byte[].class);
        Object[] arguments = cmd.toArguments(methodWithVarargsNotAtLast.getParameterTypes());
        assertEquals(3, arguments.length);

        assertEquals(randomString1, string((byte[]) arguments[0]));

        byte[][] varargs = (byte[][]) arguments[1];
        assertEquals(randomString2, string(varargs[0]));
        assertEquals(randomString3, string(varargs[1]));
        assertEquals(randomString4, string(varargs[2]));

        assertNull(arguments[2]);
    }

    @Test
    public void testToArgumentsForMethodWithMoreArgs() throws NoSuchMethodException {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"), bytes(randomString1), bytes(randomString2)});

        Method methodWithMoreArgs = RedisMethods.class
                .getDeclaredMethod("methodWithMoreArgs", byte[].class, byte[].class, byte[].class);
        Object[] arguments = cmd.toArguments(methodWithMoreArgs.getParameterTypes());
        assertEquals(3, arguments.length);

        assertEquals(randomString1, string((byte[]) arguments[0]));
        assertEquals(randomString2, string((byte[]) arguments[1]));
        assertNull(arguments[2]);
    }

    @Test
    public void testToArgumentsForMethodWithLessArgs() throws NoSuchMethodException {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        String randomString3 = RandomStringUtils.randomAscii(10);
        String randomString4 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"),
                bytes(randomString1), bytes(randomString2), bytes(randomString3), bytes(randomString4)});

        Method methodWithLessArgs = RedisMethods.class
                .getDeclaredMethod("methodWithLessArgs", byte[].class, byte[].class);
        Object[] arguments = cmd.toArguments(methodWithLessArgs.getParameterTypes());
        assertEquals(2, arguments.length);

        assertEquals(randomString1, string((byte[]) arguments[0]));
        assertEquals(randomString2, string((byte[]) arguments[1]));
    }

    @Test
    public void testToArgumentsForMethodWithOtherTypesThanBytesArray() throws NoSuchMethodException {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"), bytes(randomString1), bytes(randomString2)});

        Method methodWithOtherTypesThanBytesArray = RedisMethods.class
                .getDeclaredMethod("methodWithOtherTypesThanBytesArray", byte[].class, String.class);
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> cmd.toArguments(methodWithOtherTypesThanBytesArray.getParameterTypes()));
        assertEquals("parameter type can only be one of 'byte[]' or 'byte[][]', got String", ex.getMessage());
    }

    @Test
    public void testToString() {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(10);
        Command cmd = new Command(new byte[][]{bytes("SET"), bytes(randomString1), bytes(randomString2)});
        assertEquals(String.format(
                "\"SET\" \"%s\" \"%s\"(unknown, NEW)",
                randomString1, randomString2), cmd.toString());

        cmd.setClientAddress("1.2.3.4");
        assertEquals(String.format(
                "\"SET\" \"%s\" \"%s\"(1.2.3.4, NEW)",
                randomString1, randomString2), cmd.toString());
    }

    @Test
    public void testToStringWithLongToken() {
        String randomString1 = RandomStringUtils.randomAscii(10);
        String randomString2 = RandomStringUtils.randomAscii(MAX_PRINTED_TOKEN_LENGTH * 2);
        Command cmd = new Command(new byte[][]{bytes("SET"), bytes(randomString1), bytes(randomString2)});
        assertEquals(String.format(
                "\"SET\" \"%s\" \"%s...\"(unknown, NEW)",
                randomString1, randomString2.substring(0, MAX_PRINTED_TOKEN_LENGTH)), cmd.toString());
    }

    @Test
    public void testToShortString() {
        List<String> randomStrings = new ArrayList<>(MAX_PRINTED_ARGS * 2);
        for (int i = 0; i < MAX_PRINTED_ARGS * 2; i++) {
            randomStrings.add(RandomStringUtils.randomAscii(10));
        }
        byte[][] tokens = Stream.concat(
                        Stream.of(bytes("SET")),
                        randomStrings.stream().map(BytesUtils::bytes))
                .toArray(byte[][]::new);
        Command cmd = new Command(tokens);
        String cmdShortString = cmd.toShortString();

        StringBuilder formatBuilder = new StringBuilder("\"SET\" ");
        StringJoiner joiner = new StringJoiner(" ");
        for (int i = 0; i < MAX_PRINTED_ARGS; i++) {
            joiner.add("\"%s\"");
        }
        formatBuilder.append(joiner);
        formatBuilder.append("...(and ").append(randomStrings.size() - MAX_PRINTED_ARGS).append(" more)");
        formatBuilder.append("(unknown, NEW)");
        String format = formatBuilder.toString();
        //noinspection ConfusingArgumentToVarargsMethod
        assertEquals(String.format(format,
                        randomStrings.subList(0, MAX_PRINTED_ARGS).toArray(String[]::new)),
                cmdShortString);
    }
}
