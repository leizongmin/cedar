package com.leizm.cedar.core;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class MetaInfo {
    public final long id;
    public final KeyType type;
    public long count;
    public byte[] extra;

    public MetaInfo(long id, KeyType type, long count, byte[] extra) {
        this.id = id;
        this.type = type;
        this.count = count;
        this.extra = extra;
    }

    public static MetaInfo fromBytes(final byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        final ByteBuffer b = ByteBuffer.allocate(bytes.length);
        b.put(bytes);
        b.flip();
        final long id = b.getLong();
        final KeyType type = KeyType.fromByte(b.get());
        final long count = b.getLong(b.position());
        byte[] extra = null;
        if (b.position(17).remaining() > 0) {
            extra = new byte[b.remaining()];
            b.get(extra);
        }
        return new MetaInfo(id, type, count, extra);
    }

    public byte[] toBytes() {
        return Encoding.combineMultipleBytes(
                Encoding.longToBytes(id),
                new byte[]{type.toByte()},
                Encoding.longToBytes(count),
                extra
        );
    }

    public static class ListExtra {
        /**
         * position of next left item
         */
        public long left;

        /**
         * position of next right item
         */
        public long right;

        public ListExtra(final long left, final long right) {
            this.left = left;
            this.right = right;
        }

        public static ListExtra fromBytes(final byte[] bytes) {
            if (bytes == null) {
                return new ListExtra(0, 1);
            }
            final ByteBuffer b = ByteBuffer.allocate(bytes.length);
            b.put(bytes);
            b.flip();
            final long left = b.getLong(0);
            final long right = b.getLong(8);
            return new ListExtra(left, right);
        }

        public byte[] toBytes() {
            return Encoding.combineMultipleBytes(
                    Encoding.longToBytes(left),
                    Encoding.longToBytes(right)
            );
        }
    }

    public static class SortedListExtra {
        public long sequence;
        public int leftDeletesCount;
        public int rightDeletesCount;

        public SortedListExtra(final long sequence, final int leftDeletesCount, final int rightDeletesCount) {
            this.sequence = sequence;
            this.leftDeletesCount = leftDeletesCount;
            this.rightDeletesCount = rightDeletesCount;
        }

        public static SortedListExtra fromBytes(final byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return new SortedListExtra(0, 0, 0);
            }
            final ByteBuffer b = ByteBuffer.allocate(bytes.length);
            b.put(bytes);
            b.flip();
            final long sequence = b.getLong(0);
            final int leftDeletesCount = b.getInt(8);
            final int rightDeletesCount = b.getInt(12);
            return new SortedListExtra(sequence, leftDeletesCount, rightDeletesCount);
        }

        public byte[] toBytes() {
            return Encoding.combineMultipleBytes(
                    Encoding.longToBytes(sequence),
                    Encoding.intToBytes(leftDeletesCount),
                    Encoding.intToBytes(rightDeletesCount)
            );
        }
    }

    public static class AscSortedListExtra {
        public long sequence;
        public int deletesCount;
        public byte[] minKey;

        public AscSortedListExtra(final long sequence, final int deletesCount, final byte[] minKey) {
            this.sequence = sequence;
            this.deletesCount = deletesCount;
            this.minKey = minKey;
        }

        public static AscSortedListExtra fromBytes(final byte[] bytes) {
            if (bytes == null || bytes.length == 0) {
                return new AscSortedListExtra(0, 0, null);
            }
            final ByteBuffer b = ByteBuffer.allocate(bytes.length);
            b.put(bytes);
            b.flip();
            final long sequence = b.getLong(0);
            final int deletesCount = b.getInt(8);
            final byte[] minKey = Arrays.copyOfRange(bytes, 12, bytes.length);
            return new AscSortedListExtra(sequence, deletesCount, minKey);
        }

        public byte[] toBytes() {
            return Encoding.combineMultipleBytes(
                    Encoding.longToBytes(sequence),
                    Encoding.intToBytes(deletesCount),
                    minKey
            );
        }
    }
}
