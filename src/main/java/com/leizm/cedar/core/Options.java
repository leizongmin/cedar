package com.leizm.cedar.core;

import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;

public class Options {
    public org.rocksdb.Options rocksDBOptions;
    public int metaInfoCacheCount = 1000;

    public org.rocksdb.Options getRocksDBOptions() {
        if (rocksDBOptions == null) {
            org.rocksdb.Options options = new org.rocksdb.Options();
            options.setCreateIfMissing(true);
            options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
            options.setWriteBufferSize(1024 * 1024 * 4);
            options.setRowCache(new LRUCache(1024 * 1024 * 20));
            rocksDBOptions = options;
        }
        return rocksDBOptions;
    }
}
