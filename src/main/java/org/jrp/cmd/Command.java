package org.jrp.cmd;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.jrp.utils.BytesUtils;

import java.util.concurrent.atomic.AtomicLong;

public class Command {

    static final int MAX_PRINTED_ARGS = 7;
    static final int MAX_PRINTED_TOKEN_LENGTH = 20;

    private static final AtomicLong ID_GEN = new AtomicLong(0);

    public final long id;
    private final byte[][] tokens;
    private final CommandProcessor commandProcessor;
    private final CommandLifecycle commandLifecycle;
    private volatile String clientAddress;

    public Command() {
        this(ID_GEN.incrementAndGet());
    }

    public Command(long id) {
        this(id, null);
    }

    public Command(byte[][] tokens) {
        this(ID_GEN.incrementAndGet(), tokens);
    }

    private Command(long id, byte[][] tokens) {
        this.id = id;
        this.tokens = tokens;
        this.commandProcessor = tokens == null ? null : CommandProcessors.get(tokens[0]);
        this.commandLifecycle = new CommandLifecycle();
    }

    public CommandProcessor getCommandProcessor() {
        return commandProcessor;
    }

    public String getClientAddress() {
        return clientAddress;
    }

    public CommandLifecycle getCommandLifecycle() {
        return commandLifecycle;
    }

    public void setClientAddress(String clientAddress) {
        this.clientAddress = clientAddress;
    }

    // TODO Try to eliminate these arguments' creation.
    public Object[] toArguments(Class<?>[] types) {
        Object[] arguments = new Object[types.length];
        for (int i = 0, j = 1; i < types.length && j < tokens.length; i++, j++) {
            Class<?> type = types[i];
            if (type == byte[].class) {
                arguments[i] = tokens[j];
            } else if (type == byte[][].class) {
                int remaining = tokens.length - j;
                byte[][] remainingArguments = new byte[remaining][];
                System.arraycopy(tokens, j, remainingArguments, 0, remaining);
                arguments[i] = remainingArguments;
                break;
            } else {
                throw new IllegalStateException(
                        "parameter type can only be one of 'byte[]' or 'byte[][]', got " + type.getSimpleName());
            }
        }
        return arguments;
    }

    @Override
    public String toString() {
        return _toString(tokens.length);
    }

    public String toShortString() {
        return _toString(MAX_PRINTED_ARGS + 1);
    }

    private String _toString(int maxPrintLength) {
        return toPrettyString(maxPrintLength) +
                "(" +
                StringUtils.defaultString(clientAddress, "unknown") +
                ", " +
                commandLifecycle.getState() +
                ")";
    }

    public String toPrettyString() {
        return toPrettyString(MAX_PRINTED_ARGS + 1);
    }

    private String toPrettyString(int maxPrintLength) {
        StringBuilder sb = new StringBuilder();
        int tokensLength = tokens == null ? 0 : tokens.length;
        int l = Math.min(maxPrintLength, tokensLength);
        for (int i = 0; i < l; ++i) {
            byte[] token = tokens[i];
            if (token != null) {
                String s = BytesUtils.string(token);
                if (s.length() > MAX_PRINTED_TOKEN_LENGTH) {
                    s = s.substring(0, MAX_PRINTED_TOKEN_LENGTH).concat("...");
                }
                sb.append("\"").append(s).append("\"");
            } else {
                sb.append("\"null\"");
            }
            if (i != l - 1) {
                sb.append(" ");
            }
        }
        if (maxPrintLength < tokensLength) {
            sb.append("...(and ").append(tokensLength - maxPrintLength).append(" more)");
        }
        return sb.toString();
    }

    @VisibleForTesting
    byte[][] getTokens() {
        return tokens;
    }
}
