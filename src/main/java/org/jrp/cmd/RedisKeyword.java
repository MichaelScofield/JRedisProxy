package org.jrp.cmd;

import org.jrp.utils.BytesUtils;

import java.util.Arrays;

public enum RedisKeyword {

    AFTER,
    AGGREGATE,
    ALPHA,
    ASC,
    ASYNC,
    BEFORE,
    BY,
    CACHING,
    COUNT,
    DESC,
    DOCS,
    EX,
    EXAT,
    EXISTS,
    FAIL,
    GET,
    GETKEYS,
    GETKEYSANDFLAGS,
    GETNAME,
    GETREDIR,
    ID,
    INCRBY,
    INFO,
    KEEPTTL,
    KILL,
    LEFT,
    LEN,
    LIMIT,
    LIST,
    LOAD,
    MATCH,
    MAX,
    MAXLEN,
    MIN,
    NO,
    NX,
    OVERFLOW,
    PAUSE,
    PERSIST,
    PX,
    PXAT,
    RANK,
    REPLY,
    RESET,
    RESETSTAT,
    REWRITE,
    RIGHT,
    SAT,
    SET,
    SETNAME,
    STORE,
    SYNC,
    SUM,
    TRACKING,
    TRACKINGINFO,
    UNBLOCK,
    UNPAUSE,
    WEIGHTS,
    WITHSCORES,
    WRAP,
    XX,
    YES;

    private final byte[] name;

    RedisKeyword() {
        name = BytesUtils.bytes(this.name());
    }

    /**
     * 把byte数组转换成RedisKeyword. byte数组会变成全大写, 使用后要小心.
     */
    // TODO: Optimized to binary search.
    public static RedisKeyword convert(byte[] name) {
        if (name == null) {
            return null;
        }
        BytesUtils.toAsciiUppercase(name);
        for (RedisKeyword keyword : RedisKeyword.values()) {
            if (Arrays.equals(keyword.name, name)) {
                return keyword;
            }
        }
        return null;
    }
}
