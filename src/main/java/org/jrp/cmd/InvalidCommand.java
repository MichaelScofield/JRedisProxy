package org.jrp.cmd;

import org.apache.commons.lang3.exception.ExceptionUtils;

public class InvalidCommand extends Command {

    private final Throwable cause;

    public InvalidCommand(Throwable cause) {
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "InvalidCommand{" +
                "cause=" + ExceptionUtils.getRootCauseMessage(cause) +
                '}';
    }
}
