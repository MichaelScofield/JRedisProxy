package org.jrp.server;

import org.jrp.cmd.RWType;
import org.jrp.exception.RedisException;
import org.jrp.reply.Reply;

import static org.jrp.cmd.RWType.Type.READ;
import static org.jrp.cmd.RWType.Type.WRITE;

@SuppressWarnings("unused")
public interface RedisStringServer {

    @RWType(type = WRITE)
    Reply append(byte[] key, byte[] value) throws RedisException;

    @RWType(type = WRITE)
    Reply decr(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply decrby(byte[] key, byte[] decrement) throws RedisException;

    @RWType(type = READ)
    Reply get(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply getdel(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply getex(byte[] key, byte[][] options) throws RedisException;

    @RWType(type = READ)
    Reply getrange(byte[] key, byte[] start, byte[] end) throws RedisException;

    @RWType(type = WRITE)
    Reply getset(byte[] key, byte[] value) throws RedisException;

    @RWType(type = WRITE)
    Reply incr(byte[] key) throws RedisException;

    @RWType(type = WRITE)
    Reply incrby(byte[] key, byte[] increment) throws RedisException;

    @RWType(type = WRITE)
    Reply incrbyfloat(byte[] key, byte[] increment) throws RedisException;

    @RWType(type = READ)
    Reply mget(byte[][] keys) throws RedisException;

    @RWType(type = WRITE)
    Reply mset(byte[][] keysAndValues) throws RedisException;

    @RWType(type = WRITE)
    Reply msetnx(byte[][] keysAndValues) throws RedisException;

    @RWType(type = WRITE)
    Reply psetex(byte[] key, byte[] milliseconds, byte[] value) throws RedisException;

    @RWType(type = WRITE)
    Reply set(byte[] key, byte[] value, byte[][] options) throws RedisException;

    @RWType(type = WRITE)
    Reply setex(byte[] key, byte[] seconds, byte[] value) throws RedisException;

    @RWType(type = WRITE)
    Reply setnx(byte[] key, byte[] value) throws RedisException;

    @RWType(type = WRITE)
    Reply setrange(byte[] key, byte[] offset, byte[] value) throws RedisException;

    @RWType(type = READ)
    Reply strlen(byte[] key) throws RedisException;

    @RWType(type = READ)
    Reply substr(byte[] key, byte[] start, byte[] end) throws RedisException;
}
