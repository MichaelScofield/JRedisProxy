package org.jrp.cmd;

import org.jrp.utils.BytesUtils;

import java.util.Arrays;

// Caution: Adding a new Redis keyword must preserve the lexical ordering,
// because we use binary search to find it, see the 'convert' method.
public enum RedisKeyword {

    AFTER,
    AGGREGATE,
    ALPHA,
    ASC,
    ASYNC,
    BEFORE,
    BY,
    BYLEX,
    BYSCORE,
    CACHING,
    CH,
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
    GT,
    ID,
    INCR,
    INCRBY,
    INFO,
    KEEPTTL,
    KILL,
    LEFT,
    LEN,
    LIMIT,
    LIST,
    LOAD,
    LT,
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
    REV,
    REWRITE,
    RIGHT,
    SAT,
    SET,
    SETNAME,
    STORE,
    SUM,
    SYNC,
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
     * side effect: 'name' will be upper-cased
     */
    public static RedisKeyword convert(byte[] name) {
        if (name == null) {
            return null;
        }
        BytesUtils.toAsciiUppercase(name);

        RedisKeyword[] keywords = RedisKeyword.values();
        int l = 0, h = keywords.length - 1;
        while (l <= h) {
            int m = (l + h) >>> 1;
            int c = Arrays.compare(keywords[m].name, name);
            if (c == 0) {
                return keywords[m];
            }
            if (c < 0) {
                l = m + 1;
            } else {
                h = m - 1;
            }
        }
        return null;
    }
}
