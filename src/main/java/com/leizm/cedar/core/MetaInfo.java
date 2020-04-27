package com.leizm.cedar.core;

import java.nio.ByteBuffer;

public class MetaInfo {
    public final long objectId;
    public long size;

    public MetaInfo(long objectId, long size) {
        this.objectId = objectId;
        this.size = size;
    }

    public static MetaInfo fromBytes(final byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        final ByteBuffer b = ByteBuffer.allocate(bytes.length);
        b.put(bytes);
        b.flip();
        long objectId = b.getLong();
        long size = b.getLong(b.position());
        return new MetaInfo(objectId, size);
    }

    public byte[] toBytes() {
        return Encoding.combineMultipleBytes(Encoding.longToBytes(objectId), Encoding.longToBytes(size));
    }
}
