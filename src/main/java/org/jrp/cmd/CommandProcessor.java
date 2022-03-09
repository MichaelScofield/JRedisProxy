package org.jrp.cmd;

import com.google.common.annotations.VisibleForTesting;
import org.jrp.exception.IllegalCommandException;
import org.jrp.exception.RedisException;
import org.jrp.reply.Reply;
import org.jrp.server.RedisServer;
import org.jrp.utils.BytesUtils;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.jrp.cmd.CommandLifecycle.STATE.FINISH;
import static org.jrp.cmd.CommandLifecycle.STATE.READY;
import static org.jrp.cmd.RWType.Type.OTHER;

public class CommandProcessor {

    private final String commandName;
    private final byte[] commandNameBytes;
    private final Method commandMethod;
    private final RWType.Type rwType;
    private final Class<?>[] parameterTypes;

    public CommandProcessor(String commandName, Method commandMethod) throws IllegalCommandException {
        this.commandName = commandName;
        this.commandNameBytes = BytesUtils.bytes(commandName);
        this.commandMethod = commandMethod;
        this.rwType = getRWType(commandMethod);
        this.parameterTypes = commandMethod.getParameterTypes();
        checkValid();
    }

    @VisibleForTesting
    static RWType.Type getRWType(Method method) {
        RWType.Type type;
        RWType rwTypeAnnotation = method.getAnnotation(RWType.class);
        if (rwTypeAnnotation != null) {
            type = rwTypeAnnotation.type();
        } else {
            type = OTHER;
        }
        return type;
    }

    public Reply execute(Command command, RedisServer redisServer) throws RedisException {
        command.getCommandLifecycle().setState(READY);
        try {
            Object[] arguments = command.toArguments(parameterTypes);
            return (Reply) commandMethod.invoke(redisServer, arguments);
        } catch (Throwable e) {
            throw new RedisException(e);
        } finally {
            command.getCommandLifecycle().setState(FINISH);
        }
    }

    void checkValid() throws IllegalCommandException {
        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> type = parameterTypes[i];
            if (type == byte[].class) {
                continue;
            }
            if (type == byte[][].class) {
                if (i != parameterTypes.length - 1) {
                    throw new IllegalCommandException(String.format(
                            "Error for method %s : byte[][] parameter type can only be put at last!",
                            commandMethod.getName()));
                } else {
                    continue;
                }
            }
            throw new IllegalCommandException(String.format(
                    "Error for method %s : only byte[] or byte[][] parameter types are allowed!",
                    commandMethod.getName()));
        }
    }

    public String getCommandName() {
        return commandName;
    }

    public byte[] getCommandNameBytes() {
        return commandNameBytes;
    }

    public RWType.Type getRWType() {
        return rwType;
    }

    public Method getCommandMethod() {
        return commandMethod;
    }

    @Override
    public String toString() {
        return "CommandProcessor{" +
                "commandName='" + commandName + '\'' +
                ", rwType=" + rwType +
                ", parameterTypes=" + Arrays.toString(parameterTypes) +
                '}';
    }
}
