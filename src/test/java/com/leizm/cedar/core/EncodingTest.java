package com.leizm.cedar.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class EncodingTest {

    @Test
    public void testIsSamePrefix() {
        assertTrue(Encoding.hasPrefix("aa".getBytes(), "aa123".getBytes()));
        assertTrue(Encoding.hasPrefix("xxab".getBytes(), "xxabx".getBytes()));
        assertFalse(Encoding.hasPrefix("xxx".getBytes(), "xx".getBytes()));
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

    @Test
    public void testPrefixLowerBound() {
        assertEquals("0102037E", TestUtil.bytesToHex(Encoding.prefixLowerBound(new byte[]{1, 2, 3, 127})));
        assertEquals("01020300", TestUtil.bytesToHex(Encoding.prefixLowerBound(new byte[]{1, 2, 3, 1})));
        assertEquals("010202FF", TestUtil.bytesToHex(Encoding.prefixLowerBound(new byte[]{1, 2, 3, 0})));
        assertEquals("00FFFFFF", TestUtil.bytesToHex(Encoding.prefixLowerBound(new byte[]{1, 0, 0, 0})));
        assertNull(TestUtil.bytesToHex(Encoding.prefixLowerBound(new byte[]{0, 0, 0, 0})));
    }

    @Test
    public void testPrefixUpperBound() {
        assertEquals("01020301", TestUtil.bytesToHex(Encoding.prefixUpperBound(new byte[]{1, 2, 3, 0})));
        assertEquals("01020301", TestUtil.bytesToHex(Encoding.prefixUpperBound(new byte[]{1, 2, 3, 0})));
        assertEquals("00000002", TestUtil.bytesToHex(Encoding.prefixUpperBound(new byte[]{0, 0, 0, 1})));
        assertEquals("01020400", TestUtil.bytesToHex(Encoding.prefixUpperBound(new byte[]{1, 2, 3, -1})));
        assertEquals("02000000", TestUtil.bytesToHex(Encoding.prefixUpperBound(new byte[]{1, -1, -1, -1})));
        assertNull(TestUtil.bytesToHex(Encoding.prefixUpperBound(new byte[]{-1, -1, -1, -1})));
    }
}