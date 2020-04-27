package com.leizm.cedar.core;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Encoding {
    public static final byte[] KEYPREFIX_META = "m".getBytes();
    public static final byte[] KEYPREFIX_DATA = "d".getBytes();

    public static boolean isSamePrefix(final byte[] prefix, final byte[] key) {
        return Arrays.equals(prefix, Arrays.copyOfRange(key, 0, prefix.length));
    }

    public static byte[] longToBytes(final long v) {
        final ByteBuffer b = ByteBuffer.allocate(8);
        b.putLong(v);
        return b.array();
    }

    public static long longFromBytes(final byte[] bytes) {
        final ByteBuffer b = ByteBuffer.allocate(8);
        b.put(Arrays.copyOf(bytes, 8));
        b.flip();
        return b.getLong();
    }

    public static byte[] combineMultipleBytes(final byte[]... list) {
        int size = 0;
        for (final byte[] item : list) {
            size += item.length;
        }
        final ByteBuffer b = ByteBuffer.allocate(size);
        for (final byte[] item : list) {
            b.put(item);
        }
        return b.array();
    }

    public static byte[] stripDataKeyPrefix(byte[] fullKey) {
        return Arrays.copyOfRange(fullKey, 9, fullKey.length);
    }

    public static byte[] encodeMetaKey(byte[] key) {
        return combineMultipleBytes(KEYPREFIX_META, key);
    }

    public static byte[] encodeDataMapFieldKey(long objectId, byte[] field) {
        return combineMultipleBytes(KEYPREFIX_DATA, longToBytes(objectId), field);
    }

    public static byte[] encodeDataMapPrefixKey(long objectId) {
        return combineMultipleBytes(KEYPREFIX_DATA, longToBytes(objectId));
    }

    public static byte[] encodeDataSetKey(long objectId, byte[] value) {
        return combineMultipleBytes(KEYPREFIX_DATA, longToBytes(objectId), value);
    }

    public static byte[] decodeDataSetKey(byte[] fullKey) {
        return Arrays.copyOfRange(fullKey, 9, fullKey.length);
    }
}
