package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.exception.RedisException;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

@SuppressWarnings("unused")
public interface RedisBitmapsServer {

    @RWType(type = READ)
    Reply bitcount(byte[] key, byte[] start, byte[] end) throws RedisException;

    @RWType(type = WRITE)
    Reply bitfield(byte[] key, byte[][] options) throws RedisException;

    @RWType(type = READ)
    Reply bitfield_ro(byte[] key, byte[][] options) throws RedisException;

    @RWType(type = WRITE)
    Reply bitop(byte[] operation, byte[] destkey, byte[][] keys) throws RedisException;

    @RWType(type = READ)
    Reply bitpos(byte[] key, byte[] bit, byte[] start, byte[] end) throws RedisException;

    @RWType(type = READ)
    Reply getbit(byte[] key, byte[] offset) throws RedisException;

    @RWType(type = WRITE)
    Reply setbit(byte[] key, byte[] offset, byte[] value) throws RedisException;
}
