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
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    public static long longFromBytes(final byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static byte[] intToBytes(final int v) {
        return ByteBuffer.allocate(4).putInt(v).array();
    }

    public static long intFromBytes(final byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
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
        return ByteBuffer.wrap(bytes, 1, 8).getLong();
    }

    private static int[] bytesToUnsignedInts(final byte[] bytes) {
        final int[] ints = new int[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            ints[i] = Byte.toUnsignedInt(bytes[i]);
        }
        return ints;
    }

    private static byte[] unsignedIntsToBytes(final int[] ints) {
        final byte[] bytes = new byte[ints.length];
        for (int i = 0; i < ints.length; i++) {
            bytes[i] = (byte) ints[i];
        }
        return bytes;
    }

    public static byte[] prefixLowerBound(final byte[] prefix) {
        if (prefix.length < 1) {
            return null;
        }
        final int[] bytes = bytesToUnsignedInts(prefix);
        int i = bytes.length - 1;
        bytes[i]--;
        if (i > 0 && bytes[i] < 0) {
            bytes[i] = 255;
            bytes[--i]--;
        }
        for (; i > 0; i--) {
            if (bytes[i] < 0) {
                bytes[i] = 255;
                bytes[i - 1]--;
            }
        }

        if (bytes[0] < 0) {
            return null;
        }
        return unsignedIntsToBytes(bytes);
    }

    public static byte[] prefixUpperBound(final byte[] prefix) {
        if (prefix.length < 1) {
            return null;
        }
        final int[] bytes = bytesToUnsignedInts(prefix);
        int i = bytes.length - 1;
        bytes[i]++;
        if (i > 0 && bytes[i] > 255) {
            bytes[i] = 0;
            bytes[--i]++;
        }
        for (; i > 0; i--) {
            if (bytes[i] > 255) {
                bytes[i] = 0;
                bytes[i - 1]++;
            }
        }

        if (bytes[0] > 255) {
            return null;
        }
        return unsignedIntsToBytes(bytes);
    }
}
