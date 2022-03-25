package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

@SuppressWarnings("unused")
public interface RedisSetServer {

    @RWType(type = WRITE)
    default Reply sadd(byte[] key, byte[][] members) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply scard(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply sdiff(byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply sdiffstore(byte[] destination, byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply sinter(byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply sinterstore(byte[] destination, byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply sismember(byte[] key, byte[] member) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply smembers(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply smove(byte[] source, byte[] destination, byte[] member) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply spop(byte[] key, byte[] count) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply srandmember(byte[] key, byte[] count) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply srem(byte[] key, byte[][] members) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply sunion(byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply sunionstore(byte[] destination, byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }
}
