package org.jrp.cmd;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.exception.IllegalCommandException;
import org.jrp.server.RedisServer;

import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static org.jrp.utils.BytesUtils.*;

public class CommandProcessors {

    private static final Logger LOGGER = LogManager.getLogger(CommandProcessors.class);

    private static final CommandProcessor[][] COMMAND_PROCESSORS = new CommandProcessor[26][];

    static {
        try {
            initWith(RedisServer.class.getMethods());
        } catch (IllegalCommandException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * side effects: commandMethods will be set accessible = true
     */
    public static void initWith(Method[] commandMethods) throws IllegalCommandException {
        Arrays.fill(COMMAND_PROCESSORS, null);

        Map<Byte, List<CommandProcessor>> commandIndex = new HashMap<>();
        for (Method method : commandMethods) {
            method.setAccessible(true);

            byte[] name = bytes(method.getName());
            toAsciiUppercase(name);

            CommandProcessor processor = new CommandProcessor(string(name), method);
            commandIndex.computeIfAbsent(name[0], key -> new ArrayList<>()).add(processor);
        }
        for (Map.Entry<Byte, List<CommandProcessor>> entry : commandIndex.entrySet()) {
            COMMAND_PROCESSORS[entry.getKey() - 'A'] = entry.getValue().toArray(CommandProcessor[]::new);
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Initialized command processors: {}",
                    Arrays.stream(COMMAND_PROCESSORS)
                            .filter(Objects::nonNull)
                            .flatMap(Arrays::stream)
                            .filter(Objects::nonNull)
                            .map(CommandProcessor::toString)
                            .collect(Collectors.joining(", ")));
        }
    }

    // TODO:
    //  1. benchmark
    //  2. reorder hot commands upfront automatically
    //  3. refactor with Trie-Tree

    /**
     * side effects: 'name' will be uppercased.
     */
    public static CommandProcessor get(byte[] name) {
        if (name == null || name.length < 1) {
            return null;
        }
        return getCommandProcessor(name);
    }

    private static CommandProcessor getCommandProcessor(byte[] name) {
        toAsciiUppercase(name);

        int i = name[0] - 'A';
        if (i >= 0 && i < 26) {
            CommandProcessor[] commandIndex = COMMAND_PROCESSORS[i];
            if (commandIndex != null) {
                for (CommandProcessor command : commandIndex) {
                    if (Arrays.equals(command.getCommandNameBytes(), name)) {
                        return command;
                    }
                }
            }
        }
        return null;
    }

    public static void add(CommandProcessor processor) {
        byte[] commandNameBytes = processor.getCommandNameBytes();
        if (getCommandProcessor(commandNameBytes) != null) {
            throw new IllegalStateException(
                    "unable to add exist commandProcessor " + processor.getCommandName());
        }
        CommandProcessor[] processors = COMMAND_PROCESSORS[commandNameBytes[0] - 'A'];
        COMMAND_PROCESSORS[commandNameBytes[0] - 'A'] = processors == null ?
                new CommandProcessor[]{processor} : ArrayUtils.add(processors, processor);
        LOGGER.warn("add new commandProcessor " + processor);
    }

    public static CommandProcessor remove(byte[] name) {
        if (getCommandProcessor(name) == null) {
            throw new IllegalStateException("unable to remove not exist commandProcessor " + string(name));
        }
        CommandProcessor processor = null;
        CommandProcessor[] processors = COMMAND_PROCESSORS[name[0] - 'A'];
        if (processors != null) {
            for (int i = 0; i < processors.length; i++) {
                if (Arrays.equals(processors[i].getCommandNameBytes(), name)) {
                    processor = processors[i];
                    COMMAND_PROCESSORS[name[0] - 'A'] = ArrayUtils.remove(processors, i);
                    LOGGER.warn("remove commandProcessor " + processor);
                    break;
                }
            }
        }
        return processor;
    }

    @VisibleForTesting
    static CommandProcessor[][] getCommandProcessors() {
        return COMMAND_PROCESSORS;
    }
}
