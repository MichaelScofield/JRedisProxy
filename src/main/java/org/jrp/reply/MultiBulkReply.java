package org.jrp.reply;

import io.lettuce.core.KeyValue;
import io.lettuce.core.ScoredValue;
import io.netty.buffer.ByteBuf;

import java.util.*;
import java.util.stream.Collectors;

import static org.jrp.utils.BytesUtils.bytes;

public record MultiBulkReply(Reply[] replies) implements Reply {

    private static final char MARKER = '*';

    public static MultiBulkReply fromBytesMap(Map<byte[], byte[]> map) {
        if (map == null || map.size() < 1) {
            return multiBulkReply(Collections.emptyList());
        }
        BulkReply[] replies = new BulkReply[map.size() * 2];
        int i = 0;
        for (Map.Entry<byte[], byte[]> entry : map.entrySet()) {
            replies[i++] = BulkReply.bulkReply(entry.getKey());
            replies[i++] = BulkReply.bulkReply(entry.getValue());
        }
        return new MultiBulkReply(replies);
    }

    public static MultiBulkReply fromStringMap(Map<String, String> map) {
        if (map == null || map.size() < 1) {
            return multiBulkReply(Collections.emptyList());
        }
        BulkReply[] replies = new BulkReply[map.size() * 2];
        int i = 0;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            replies[i++] = BulkReply.bulkReply(entry.getKey());
            replies[i++] = BulkReply.bulkReply(entry.getValue());
        }
        return new MultiBulkReply(replies);
    }

    public static MultiBulkReply fromScoreValues(List<ScoredValue<byte[]>> scoredValues) {
        if (scoredValues == null || scoredValues.size() < 1) {
            return multiBulkReply(Collections.emptyList());
        }
        BulkReply[] replies = new BulkReply[scoredValues.size() * 2];
        int i = 0;
        for (ScoredValue<byte[]> scoredValue : scoredValues) {
            replies[i++] = BulkReply.bulkReply(scoredValue.getValue());
            replies[i++] = BulkReply.bulkReply(scoredValue.getScore());
        }
        return new MultiBulkReply(replies);
    }

    public static MultiBulkReply fromKeyValues(List<KeyValue<byte[], byte[]>> keyValues) {
        if (keyValues == null || keyValues.size() < 1) {
            return multiBulkReply(Collections.emptyList());
        }
        BulkReply[] replies = keyValues.stream()
                .map(keyValue -> BulkReply.bulkReply(keyValue.hasValue() ? keyValue.getValue() : null))
                .toArray(BulkReply[]::new);
        return new MultiBulkReply(replies);
    }

    public static MultiBulkReply fromIntegers(List<Long> integers) {
        if (integers == null || integers.isEmpty()) {
            return multiBulkReply(Collections.emptyList());
        }
        IntegerReply[] replies = integers.stream()
                .map(IntegerReply::integer)
                .toArray(IntegerReply[]::new);
        return new MultiBulkReply(replies);
    }

    public static MultiBulkReply multiBulkReply(Collection<String> strings) {
        BulkReply[] replies = strings.stream()
                .map(BulkReply::bulkReply)
                .toArray(BulkReply[]::new);
        return new MultiBulkReply(replies);
    }

    public static MultiBulkReply rawMultiBulkReply(Collection<byte[]> bytes) {
        BulkReply[] replies = bytes.stream()
                .map(BulkReply::bulkReply)
                .toArray(BulkReply[]::new);
        return new MultiBulkReply(replies);
    }

    @Override
    public void write(ByteBuf out) {
        out.writeByte(MARKER);
        int len = replies == null ? -1 : replies.length;
        out.writeBytes(bytes(String.valueOf(len)));
        out.writeBytes(CRLF);
        if (replies != null) {
            for (Reply reply : replies) {
                reply.write(out);
            }
        }
    }

    @Override
    public String toString() {
        if (replies == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(Arrays.stream(replies)
                .map(reply -> "\"" + reply + "\"")
                .limit(10)
                .collect(Collectors.joining(" ")));
        if (replies.length > 10) {
            sb.append(" ...(and ").append(replies.length - 10).append(" more)");
        }
        return sb.toString();
    }
}
