package com.leizm.cedar.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MetaInfoTest {

    @Test
    void test() {
        MetaInfo m = new MetaInfo(123, KeyType.Map, 456);
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 123, 1, 0, 0, 0, 0, 0, 0, 1, -56}, m.toBytes());
        MetaInfo m2 = MetaInfo.fromBytes(m.toBytes());
        assertEquals(123, m2.objectId);
        assertEquals(456, m2.size);
        assertEquals(KeyType.Map, m2.type);
    }
}