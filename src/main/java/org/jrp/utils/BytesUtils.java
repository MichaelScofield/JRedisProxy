package org.jrp.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class BytesUtils {

    private static final byte[] NEG_INF = "-inf".getBytes(StandardCharsets.UTF_8);
    private static final byte[] POS_INF = "+inf".getBytes(StandardCharsets.UTF_8);

    public static String string(byte[] bytes) {
        return bytes == null ? "null" : new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] bytes(String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

    public static int toInt(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("unable to convert null bytes to int");
        }
        return Integer.parseInt(string(bytes));
    }

    public static long toLong(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("unable to convert null bytes to long");
        }
        return Long.parseLong(string(bytes));
    }

    public static double toDouble(byte[] bytes) {
        if (Arrays.equals(NEG_INF, bytes)) {
            return Double.NEGATIVE_INFINITY;
        } else if (Arrays.equals(POS_INF, bytes)) {
            return Double.POSITIVE_INFINITY;
        } else {
            return Double.parseDouble(string(bytes));
        }
    }

    public static void toAsciiUppercase(byte[] name) {
        for (int i = 0, l = name.length; i < l; i++) {
            byte b = name[i];
            if (b >= 'a' && b <= 'z') {
                name[i] = (byte) (b & 0b11011111);
            }
        }
    }
}
