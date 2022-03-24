package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;
import static org.jrp.utils.BytesUtils.bytes;

@SuppressWarnings("unused")
public interface RedisListServer {

    @RWType(type = READ)
    default Reply lindex(byte[] key, byte[] index) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply linsert(byte[] key, byte[] where, byte[] pivot, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply llen(byte[] key) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply lmove(byte[] source, byte[] destination, byte[] whereFrom, byte[] whereTo) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply lpop(byte[] key, byte[] count) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply rpop(byte[] key, byte[] count) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply lpos(byte[] key, byte[] element, byte[][] args) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply lpush(byte[] key, byte[][] elements) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply rpush(byte[] key, byte[][] elements) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply lpushx(byte[] key, byte[][] elements) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply rpushx(byte[] key, byte[][] elements) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply lrange(byte[] key, byte[] start, byte[] stop) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply lrem(byte[] key, byte[] count, byte[] element) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply lset(byte[] key, byte[] index, byte[] element) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply ltrim(byte[] key, byte[] start, byte[] stop) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply rpoplpush(byte[] source, byte[] destination) {
        return lmove(source, destination, bytes("RIGHT"), bytes("LEFT"));
    }
}
