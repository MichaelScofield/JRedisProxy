package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

// TODO Implement all commands in Generic command group.
@SuppressWarnings("unused")
public interface RedisGenericServer {

    @RWType(type = WRITE)
    default Reply del(byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply exists(byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply expire(byte[] key, byte[] seconds) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply expireat(byte[] key, byte[] timestamp) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply persist(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply pexpire(byte[] key, byte[] milliseconds) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply pexpireat(byte[] key, byte[] millisecondsTimestamp) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply pttl(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply rename(byte[] key, byte[] newkey) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply renamenx(byte[] key, byte[] newkey) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply sort(byte[] key, byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply ttl(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply type(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }
}
