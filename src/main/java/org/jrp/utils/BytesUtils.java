package org.jrp.utils;

import java.nio.charset.StandardCharsets;

public final class BytesUtils {

    private static final String NEG_INF = "-inf";
    private static final String POS_INF = "+inf";

    public static String string(byte[] bytes) {
        return bytes == null ? "null" : new String(bytes, StandardCharsets.UTF_8);
    }

    public static String string(byte[] bytes, int offset, int length) {
        return bytes == null ? "null" : new String(bytes, offset, length, StandardCharsets.UTF_8);
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
        return toDouble(string(bytes));
    }

    public static double toDouble(byte[] bytes, int offset, int length) {
        return toDouble(string(bytes, offset, length));
    }

    private static double toDouble(String s) {
        if (NEG_INF.equals(s)) {
            return Double.NEGATIVE_INFINITY;
        } else if (POS_INF.equals(s)) {
            return Double.POSITIVE_INFINITY;
        } else {
            return Double.parseDouble(s);
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
