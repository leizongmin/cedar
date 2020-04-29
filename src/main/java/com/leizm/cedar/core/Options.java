package com.leizm.cedar.core;

import org.iq80.leveldb.CompressionType;

public class Options {
    public Long cacheSize;
    public Integer writeBufferSize = 4 << 20;
    public Integer blockSize = 4 * 1024;
    public Integer blockRestartInterval = 16;
    public Boolean compression = true;
    public Boolean verifyChecksums = true;

    public int metaInfoCacheCount = 1000;

    /**
     * returns DBOptions of LevelDB, if Options is null returns an empty DBOptions
     *
     * @param options
     * @return
     */
    public static org.iq80.leveldb.Options getOrDefaultLevelDBOptions(Options options) {
        return options == null ? new Options().getLevelDBOptions() : options.getLevelDBOptions();
    }

    /**
     * returns DBOptions of LevelDB
     *
     * @return
     */
    public org.iq80.leveldb.Options getLevelDBOptions() {
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
