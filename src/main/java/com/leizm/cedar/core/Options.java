package com.leizm.cedar.core;

import org.iq80.leveldb.CompressionType;

public class Options {
    public Long cacheSize;
    public Integer writeBufferSize;
    public Integer blockSize;
    public Integer blockRestartInterval;
    public Boolean compression;
    public Boolean verifyChecksums;

    public static org.iq80.leveldb.Options getOrDefaultLevelDBOptions(Options options) {
        return options == null ? new Options().toLevelDBOptions() : options.toLevelDBOptions();
    }

    public org.iq80.leveldb.Options toLevelDBOptions() {
        org.iq80.leveldb.Options options = new org.iq80.leveldb.Options().createIfMissing(true);
        if (cacheSize != null) {
            options.cacheSize(cacheSize);
        }
        if (writeBufferSize != null) {
            options.writeBufferSize(writeBufferSize);
        }
        if (blockSize != null) {
            options.blockSize(blockSize);
        }
        if (blockRestartInterval != null) {
            options.blockRestartInterval(blockRestartInterval);
        }
        if (compression != null) {
            options.compressionType(CompressionType.SNAPPY);
        }
        if (verifyChecksums != null) {
            options.verifyChecksums(verifyChecksums);
        }
        return options;
    }
}
