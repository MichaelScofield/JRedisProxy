package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

@SuppressWarnings("unused")
public interface RedisSortedSetServer {

    @RWType(type = WRITE)
    default Reply zadd(byte[] key, byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zcard(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zcount(byte[] key, byte[] min, byte[] max) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply zincrby(byte[] key, byte[] increment, byte[] member) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply zinterstore(byte[] destination, byte[] numkeys, byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zrange(byte[] key, byte[] min, byte[] max, byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zrangebyscore(byte[] key, byte[] min, byte[] max, byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zrank(byte[] key, byte[] member) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply zrem(byte[] key, byte[][] members) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply zremrangebyrank(byte[] key, byte[] start, byte[] stop) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply zremrangebyscore(byte[] key, byte[] min, byte[] max) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zrevrank(byte[] key, byte[] member) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zrevrange(byte[] key, byte[] start, byte[] stop, byte[] withscores) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zrevrangebyscore(byte[] key, byte[] max, byte[] min, byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zscan(byte[] key, byte[] cursor, byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply zscore(byte[] key, byte[] member) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply zunionstore(byte[] destination, byte[] numkeys, byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }
}
