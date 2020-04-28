package com.leizm.cedar.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MetaInfoTest {

    @Test
    void test() {
        final MetaInfo m = new MetaInfo(123, KeyType.Map, 456, null);
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 123, 1, 0, 0, 0, 0, 0, 0, 1, -56}, m.toBytes());
        final MetaInfo m2 = MetaInfo.fromBytes(m.toBytes());
        assertEquals(123, m2.objectId);
        assertEquals(456, m2.size);
        assertEquals(KeyType.Map, m2.type);
        assertNull(m2.extra);

        final MetaInfo m3 = new MetaInfo(456, KeyType.Set, 789, new byte[]{6, 7, 8});
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 1, -56, 2, 0, 0, 0, 0, 0, 0, 3, 21, 6, 7, 8}, m3.toBytes());
        final MetaInfo m4 = MetaInfo.fromBytes(m3.toBytes());
        assertEquals(456, m4.objectId);
        assertEquals(789, m4.size);
        assertEquals(KeyType.Set, m4.type);
        assertArrayEquals(new byte[]{6, 7, 8}, m4.extra);
    }
}