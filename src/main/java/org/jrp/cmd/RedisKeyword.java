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
    COUNT,
    DESC,
    EX,
    EXAT,
    EXISTS,
    GET,
    KEEPTTL,
    LIMIT,
    LIST,
    LOAD,
    MATCH,
    MAX,
    MIN,
    NX,
    PERSIST,
    PX,
    PXAT,
    SET,
    STORE,
    SUM,
    WEIGHTS,
    WITHSCORES,
    XX;

    private final byte[] name;

    RedisKeyword() {
        name = BytesUtils.bytes(this.name());
    }

    /**
     * 把byte数组转换成RedisKeyword. byte数组会变成全大写, 使用后要小心.
     */
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
