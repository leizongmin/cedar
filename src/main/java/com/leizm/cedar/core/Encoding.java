package com.leizm.cedar.core;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Encoding {
    public static final byte[] KEY_PREFIX_META = "m".getBytes();
    public static final byte[] KEY_PREFIX_DATA = "d".getBytes();

    public static boolean hasPrefix(final byte[] prefix, final byte[] key) {
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
            if (item != null) {
                size += item.length;
            }
        }
        final ByteBuffer b = ByteBuffer.allocate(size);
        for (final byte[] item : list) {
            if (item != null) {
                b.put(item);
            }
        }
        return b.array();
    }

    public static byte[] stripDataKeyPrefix(final byte[] fullKey) {
        return Arrays.copyOfRange(fullKey, 9, fullKey.length);
    }

    public static byte[] encodeMetaKey(final byte[] key) {
        return combineMultipleBytes(KEY_PREFIX_META, key);
    }

    public static byte[] encodeDataMapFieldKey(final long keyId, final byte[] field) {
        return combineMultipleBytes(KEY_PREFIX_DATA, longToBytes(keyId), field);
    }

    public static byte[] encodeDataMapPrefixKey(final long keyId) {
        return combineMultipleBytes(KEY_PREFIX_DATA, longToBytes(keyId));
    }

    public static byte[] encodeDataSetKey(final long keyId, final byte[] value) {
        return combineMultipleBytes(KEY_PREFIX_DATA, longToBytes(keyId), value);
    }

    public static byte[] decodeDataSetKey(final byte[] fullKey) {
        return Arrays.copyOfRange(fullKey, 9, fullKey.length);
    }

    public static byte[] encodeDataSortedListKey(final long keyId, final long seq, final byte[] score) {
        return combineMultipleBytes(KEY_PREFIX_DATA, longToBytes(keyId), score, longToBytes(seq));
    }

    public static byte[] encodeDataSortedListPrefixKey(final long keyId) {
        return combineMultipleBytes(KEY_PREFIX_DATA, longToBytes(keyId));
    }

    public static byte[] decodeDataSortedListKey(final byte[] fullKey) {
        return Arrays.copyOfRange(fullKey, 9, 17);
    }

    public static int compareScoreBytes(final byte[] score1, final byte[] score2) {
        if (score1.length != score2.length) {
            return 0;
        }
        for (int i = 0; i < score1.length; i++) {
            int ret = Byte.compare(score1[i], score2[i]);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }

    public static byte[] encodeDataListKey(final long keyId, final long position) {
        return combineMultipleBytes(KEY_PREFIX_DATA, longToBytes(keyId), comparableLongToBytes(position));
    }

    public static byte[] comparableLongToBytes(final long v) {
        final ByteBuffer b = ByteBuffer.allocate(9);
        b.put((byte) (v >= 0 ? '>' : '<'));
        b.putLong(v);
        return b.array();
    }

    public static long comparableLongFromBytes(final byte[] bytes) {
        final ByteBuffer b = ByteBuffer.allocate(8);
        b.put(Arrays.copyOfRange(bytes, 1, 9));
        b.flip();
        return b.getLong();
    }
}
