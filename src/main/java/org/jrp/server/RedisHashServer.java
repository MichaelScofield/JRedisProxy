package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

@SuppressWarnings("unused")
public interface RedisHashServer {

    @RWType(type = WRITE)
    default Reply hdel(byte[] key, byte[][] fields) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply hexists(byte[] key, byte[] field) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply hget(byte[] key, byte[] field) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply hgetall(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply hincrby(byte[] key, byte[] field, byte[] increment) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply hincrbyfloat(byte[] key, byte[] field, byte[] increment) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply hkeys(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply hlen(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply hmget(byte[] key, byte[][] fields) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply hset(byte[] key, byte[][] fieldsAndValues) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply hmset(byte[] key, byte[][] fieldsAndValues) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply hscan(byte[] key, byte[] cursor, byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply hsetnx(byte[] key, byte[] field, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply hvals(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }
}
