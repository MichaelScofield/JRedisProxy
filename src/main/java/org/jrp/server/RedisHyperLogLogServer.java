package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.reply.ErrorReply;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.WRITE;

@SuppressWarnings("unused")
public interface RedisHyperLogLogServer {

    @RWType(type = WRITE)
    default Reply pfadd(byte[] key, byte[][] elements) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply pfcount(byte[][] keys) {
        return ErrorReply.NOT_IMPL;
    }

    @RWType(type = WRITE)
    default Reply pfmerge(byte[] destkey, byte[][] sourceKeys) {
        return ErrorReply.NOT_IMPL;
    }
}
