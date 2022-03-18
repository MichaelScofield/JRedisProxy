package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

@SuppressWarnings("unused")
public interface RedisStringServer {

    @RWType(type = WRITE)
    default Reply append(byte[] key, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply decr(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply decrby(byte[] key, byte[] decrement) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply get(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply getdel(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply getex(byte[] key, byte[][] options) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply getrange(byte[] key, byte[] start, byte[] end) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply getset(byte[] key, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply incr(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply incrby(byte[] key, byte[] increment) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply incrbyfloat(byte[] key, byte[] increment) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply mget(byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply mset(byte[][] keysAndValues) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply msetnx(byte[][] keysAndValues) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply psetex(byte[] key, byte[] milliseconds, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply set(byte[] key, byte[] value, byte[][] options) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply setex(byte[] key, byte[] seconds, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply setnx(byte[] key, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply setrange(byte[] key, byte[] offset, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply strlen(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply substr(byte[] key, byte[] start, byte[] end) {
        return getrange(key, start, end);
    }
}
