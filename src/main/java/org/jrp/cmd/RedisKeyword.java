package org.jrp.cmd;

import org.jrp.utils.BytesUtils;

import java.util.Arrays;

public enum RedisKeyword {

    AFTER,
    AGGREGATE,
    ALPHA,
    ASC,
    BEFORE,
    BY,
    CACHING,
    COUNT,
    DESC,
    EX,
    EXAT,
    EXISTS,
    FAIL,
    GET,
    GETNAME,
    GETREDIR,
    ID,
    INCRBY,
    INFO,
    KEEPTTL,
    KILL,
    LIMIT,
    LIST,
    LOAD,
    MATCH,
    MAX,
    MIN,
    NO,
    NX,
    OVERFLOW,
    PAUSE,
    PERSIST,
    PX,
    PXAT,
    REPLY,
    SAT,
    SET,
    SETNAME,
    STORE,
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
