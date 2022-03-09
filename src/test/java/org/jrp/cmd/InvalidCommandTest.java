package org.jrp.cmd;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

public class InvalidCommandTest {

    @Test
    public void testGetCause() {
        Exception cause = new Exception(RandomStringUtils.randomAscii(10));
        InvalidCommand invalidCommand = new InvalidCommand(cause);
        Throwable commandCause = invalidCommand.getCause();
        assertSame(cause, commandCause);
    }
}
