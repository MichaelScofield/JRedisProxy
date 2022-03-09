package org.jrp.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jrp.cmd.Command;
import org.jrp.reply.Reply;
import org.jrp.server.handler.RedisCommandHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

// TODO use something like Disruptor
public class RedisCommandsQueue {

    private static final Logger LOGGER = LogManager.getLogger(RedisCommandsQueue.class);

    private final ConcurrentMap<ChannelId, ConcurrentSkipListMap<Long, Pair<Command, Reply>>> reqRespQueues =
            new ConcurrentHashMap<>();

    final RedisCommandHandler redisCommandHandler;

    public RedisCommandsQueue(RedisCommandHandler redisCommandHandler) {
        this.redisCommandHandler = redisCommandHandler;
    }

    public void onChannelActive(Channel channel) {
        ConcurrentSkipListMap<Long, Pair<Command, Reply>> queue = new ConcurrentSkipListMap<>();
        ChannelId id = channel.id();
        reqRespQueues.put(id, queue);
    }

    public void onChannelInactive(Channel channel) {
        ChannelId id = channel.id();
        reqRespQueues.remove(id);
    }

    public void add(Channel channel, Command command) {
        ConcurrentSkipListMap<Long, Pair<Command, Reply>> queue = reqRespQueues.get(channel.id());
        queue.put(command.id, MutablePair.of(command, null));
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("channel {} add pending command '{}'", channel, command);
        }
    }

    public void set(Channel channel, long commandId, Reply reply) {
        ConcurrentSkipListMap<Long, Pair<Command, Reply>> queue = reqRespQueues.get(channel.id());
        if (queue == null) {
            return;
        }
        queue.get(commandId).setValue(reply);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("channel {} set reply '{}' to command id {}", channel, reply, commandId);
        }
    }

    public void flushReadyReplies(Channel channel) {
        ConcurrentSkipListMap<Long, Pair<Command, Reply>> queue = reqRespQueues.get(channel.id());
        if (queue == null) {
            return;
        }
        synchronized (channel.id()) {
            while (!Thread.currentThread().isInterrupted()) {
                Map.Entry<Long, Pair<Command, Reply>> firstEntry = queue.firstEntry();
                if (firstEntry == null) {
                    break;
                }
                Pair<Command, Reply> pair = firstEntry.getValue();
                Reply readyReply = pair.getValue();
                if (readyReply == null) {
                    break;
                }
                Command command = pair.getKey();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("channel {} flushing reply '{}' to command '{}'", channel, readyReply, command);
                }
                redisCommandHandler.flush(channel, command, readyReply);
                Map.Entry<Long, Pair<Command, Reply>> _firstEntry = queue.pollFirstEntry();
                // safe because command id is strictly increment
                // and a command is handled by only one thread, i.e. the Lettuce thread
                if (!firstEntry.getKey().equals(_firstEntry.getKey())) {
                    throw new AssertionError("command is not handled single-threaded?");
                }
            }
        }
    }
}
