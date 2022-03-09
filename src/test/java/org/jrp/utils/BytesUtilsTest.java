package org.jrp.utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class BytesUtilsTest {

    @Test
    public void testBytesToString() {
        assertEquals("null", BytesUtils.string(null));
        String s = RandomStringUtils.randomAlphabetic(10);
        assertEquals(s, BytesUtils.string(s.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testStringToBytes() {
        String s = RandomStringUtils.randomAlphabetic(10);
        assertArrayEquals(s.getBytes(StandardCharsets.UTF_8), BytesUtils.bytes(s));
    }

    @Test
    public void testBytesToInt() {
        int i = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        assertEquals(i, BytesUtils.toInt(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
        int j = -RandomUtils.nextInt(0, Integer.MAX_VALUE);
        assertEquals(j, BytesUtils.toInt(String.valueOf(j).getBytes(StandardCharsets.UTF_8)));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> BytesUtils.toInt(null));
        assertEquals("unable to convert null bytes to int", ex.getMessage());
    }

    @Test
    public void testBytesToLong() {
        long i = RandomUtils.nextLong(0, Long.MAX_VALUE);
        assertEquals(i, BytesUtils.toLong(String.valueOf(i).getBytes(StandardCharsets.UTF_8)));
        long j = -RandomUtils.nextLong(0, Long.MAX_VALUE);
        assertEquals(j, BytesUtils.toLong(String.valueOf(j).getBytes(StandardCharsets.UTF_8)));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> BytesUtils.toLong(null));
        assertEquals("unable to convert null bytes to long", ex.getMessage());
    }

    @Test
    public void testToDouble() {
        double d = RandomUtils.nextDouble();
        assertEquals(d, BytesUtils.toDouble(String.valueOf(d).getBytes(StandardCharsets.UTF_8)));
        assertEquals(Double.NEGATIVE_INFINITY, BytesUtils.toDouble(
                String.valueOf(Double.NEGATIVE_INFINITY).getBytes(StandardCharsets.UTF_8)));
        assertEquals(Double.POSITIVE_INFINITY, BytesUtils.toDouble(
                String.valueOf(Double.POSITIVE_INFINITY).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testToAsciiUppercase() {
        byte[] name = new byte[]{'g', 'e', 't'};
        BytesUtils.toAsciiUppercase(name);
        assertArrayEquals(new byte[]{'G', 'E', 'T'}, name);
    }
}
