package com.leizm.cedar.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EncodingTest {

    @Test
    public void testIsSamePrefix() {
        assertTrue(Encoding.isSamePrefix("aa".getBytes(), "aa123".getBytes()));
        assertTrue(Encoding.isSamePrefix("xxab".getBytes(), "xxabx".getBytes()));
        assertFalse(Encoding.isSamePrefix("xxx".getBytes(), "xx".getBytes()));
    }

    @Test
    public void testLongToBytes() {
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 123}, Encoding.longToBytes(123));
    }

    @Test
    public void testLongFromBytes() {
        assertEquals(123, Encoding.longFromBytes(new byte[]{0, 0, 0, 0, 0, 0, 0, 123}));
    }

    @Test
    public void testCombineMultipleBytes() {
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, Encoding.combineMultipleBytes(new byte[]{}, new byte[]{1, 2}, new byte[]{3}, new byte[]{4, 5}));
    }

    @Test
    public void testEncodeMetaKey() {
        assertArrayEquals(new byte[]{109, 1, 2, 3}, Encoding.encodeMetaKey(new byte[]{1, 2, 3}));
    }

    @Test
    public void testEncodeDataMapFieldKey() {
        assertArrayEquals(new byte[]{100, 0, 0, 0, 0, 0, 0, 0, 111, 4, 5}, Encoding.encodeDataMapFieldKey(111, new byte[]{4, 5}));
    }

    @Test
    public void testEncodeDataMapPrefixKey() {
        assertArrayEquals(new byte[]{100, 0, 0, 0, 0, 0, 0, 0, 111}, Encoding.encodeDataMapPrefixKey(111));
    }
}