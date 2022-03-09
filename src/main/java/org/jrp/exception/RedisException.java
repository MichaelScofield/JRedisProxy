package org.jrp.exception;

public class RedisException extends Exception {

    public static final RedisException SYNTAX_ERROR = new RedisException("syntax error");
    public static final RedisException NOT_IMPLEMENTED_ERROR = new RedisException("not implemented error");

    public RedisException(Throwable cause) {
        super(cause);
    }

    public RedisException(String message) {
        super(message);
    }
}
