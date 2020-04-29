package com.leizm.cedar.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class LRUCacheTest {

    String getKey(final long v) {
        return new String(Encoding.longToBytes(v));
    }

    @Test
    void test() {
        LRUCache<String, MetaInfo> cache = new LRUCache<>(10);
        assertEquals(10, cache.capacity());
        assertEquals(0, cache.size());
        for (long i = 0; i < 10; i++) {
            cache.put(getKey(i), new MetaInfo(i, KeyType.List, 0, null));
        }
        assertEquals(10, cache.size());
        for (long i = 0; i < 10; i++) {
            assertNotNull(cache.get(getKey(i)));
        }

        for (long i = 0; i < 100; i++) {
            cache.put(getKey(i), new MetaInfo(i, KeyType.List, 0, null));
        }
        assertEquals(10, cache.size());
        for (long i = 0; i < 90; i++) {
            assertNull(cache.get(getKey(i)));
        }
        for (long i = 90; i < 100; i++) {
            assertNotNull(cache.get(getKey(i)));
        }

        cache.clear();
        assertEquals(10, cache.capacity());
        assertEquals(0, cache.size());
    }
}