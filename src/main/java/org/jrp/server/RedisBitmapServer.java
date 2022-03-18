package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

@SuppressWarnings("unused")
public interface RedisBitmapServer {

    @RWType(type = READ)
    default Reply bitcount(byte[] key, byte[] start, byte[] end) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply bitfield(byte[] key, byte[][] options) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply bitfield_ro(byte[] key, byte[][] options) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply bitop(byte[] operation, byte[] destkey, byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply bitpos(byte[] key, byte[] bit, byte[] start, byte[] end) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = READ)
    default Reply getbit(byte[] key, byte[] offset) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply setbit(byte[] key, byte[] offset, byte[] value) {
        return ErrorReply.NOT_IMPL;
    }
}
