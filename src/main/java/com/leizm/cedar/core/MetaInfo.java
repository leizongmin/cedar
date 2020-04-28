package com.leizm.cedar.core;

import java.nio.ByteBuffer;

public class MetaInfo {
    public final long objectId;
    public final KeyType type;
    public long size;
    public byte[] extra;

    public MetaInfo(long objectId, KeyType type, long size, byte[] extra) {
        this.objectId = objectId;
        this.type = type;
        this.size = size;
        this.extra = extra;
    }

    public static MetaInfo fromBytes(final byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        final ByteBuffer b = ByteBuffer.allocate(bytes.length);
        b.put(bytes);
        b.flip();
        final long objectId = b.getLong();
        final KeyType type = KeyType.fromByte(b.get());
        final long size = b.getLong(b.position());
        byte[] extra = null;
        if (b.position(17).remaining() > 0) {
            extra = new byte[b.remaining()];
            b.get(extra);
        }
        return new MetaInfo(objectId, type, size, extra);
    }

    public byte[] toBytes() {
        return Encoding.combineMultipleBytes(
                Encoding.longToBytes(objectId),
                new byte[]{type.toByte()},
                Encoding.longToBytes(size),
                extra
        );
    }
}
