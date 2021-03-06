package com.leizm.cedar.core;

import org.rocksdb.*;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Database implements IDatabase {
    /**
     * LevelDB database instance
     */
    protected RocksDB db;

    /**
     * database path
     */
    protected String path;

    /**
     * next key id
     */
    protected long nextKeyId = 0;

    protected LRUCache<String, MetaInfo> metaInfoCache;

    /**
     * open database
     *
     * @param path    store path
     * @param options options
     * @throws RocksDBException
     */
    public Database(String path, Options options) throws RocksDBException {
        options = options == null ? new Options() : options;
        this.db = RocksDB.open(options.getRocksDBOptions(), path);
        this.path = path;
        this.metaInfoCache = new LRUCache<>(options.metaInfoCacheCount);
        initAfterOpen();
    }

    /**
     * open database
     *
     * @param path store path
     * @throws RocksDBException
     */
    public Database(String path) throws RocksDBException {
        this(path, null);
    }

    /**
     * returns LevelDB instance
     *
     * @return RocksDB
     */
    public RocksDB getDb() {
        return db;
    }

    /**
     * returns database path
     *
     * @return path
     */
    public String getPath() {
        return path;
    }

    /**
     * close database
     *
     */
    public void close() {
        db.close();
    }

    protected void initAfterOpen() {
        final Box<Long> maxKeyId = Box.of(1L);
        prefixForEach(Encoding.KEY_PREFIX_META, (entry -> {
            final MetaInfo meta = MetaInfo.fromBytes(entry.value());
            if (meta.id > maxKeyId.value) {
                maxKeyId.value = meta.id;
            }
        }));
        nextKeyId = maxKeyId.value + 1;
    }

    protected long prefixForEach(final byte[] prefix, final Consumer<RocksIterator> onItem) {
        long count = 0;
        try (final ReadOptions readOptions = dbReadOptions(o -> {
            o.setPrefixSameAsStart(true);
            o.setTotalOrderSeek(false);
        })) {
            try (final RocksIterator it = dbIterator(readOptions)) {
                it.seek(prefix);
                while (it.isValid()) {
                    if (!Encoding.hasPrefix(prefix, it.key())) {
                        break;
                    }
                    onItem.accept(it);
                    count++;
                    it.next();
                }
            }
        }
        return count;
    }

    protected Slice toDBSlice(final byte[] key) {
        return new Slice(key);
    }

    protected ReadOptions dbReadOptions(Consumer<ReadOptions> setup) {
        final ReadOptions readOptions = new ReadOptions();
        if (setup != null) {
            setup.accept(readOptions);
        }
        return readOptions;
    }

    protected RocksIterator dbIterator(ReadOptions readOptions) {
        return readOptions == null ? db.newIterator() : db.newIterator(readOptions);
    }

    protected byte[] dbGet(byte[] key) {
        // System.out.printf("GET %s\n", new String(key));
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            return null;
        }
    }

    protected void dbPut(byte[] key, byte[] value) {
        // System.out.printf("PUT %s = %s\n", new String(key), new String(value));
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    protected void dbDelete(byte[] key) {
        // System.out.printf("DELETE %s\n", new String(key));
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    protected MetaInfo getKeyMeta(byte[] key) {
        final byte[] fullKey = Encoding.encodeMetaKey(key);
        return MetaInfo.fromBytes(dbGet(fullKey));
    }

    protected MetaInfo getOrCreateKeyMeta(byte[] key, KeyType type) {
        final String cacheKey = new String(key);
        MetaInfo meta = metaInfoCache.get(cacheKey);
        if (meta != null) {
            return meta;
        }
        final byte[] fullKey = Encoding.encodeMetaKey(key);
        meta = MetaInfo.fromBytes(dbGet(fullKey));
        if (meta == null) {
            meta = new MetaInfo(nextKeyId++, type, 0, null);
            dbPut(fullKey, meta.toBytes());
        } else if (!meta.type.equals(type)) {
            throw new IllegalArgumentException(String.format("expected type %s but actually %s", type.name(), meta.type.name()));
        }
        metaInfoCache.put(cacheKey, meta);
        return meta;
    }

    protected void updateMetaInfo(byte[] key, MetaInfo meta) {
        if (meta.count > 0) {
            metaInfoCache.put(new String(key), meta);
            dbPut(Encoding.encodeMetaKey(key), meta.toBytes());
        } else {
            metaInfoCache.remove(new String(key));
            dbDelete(Encoding.encodeMetaKey(key));
        }
    }

    protected long getCount(final byte[] key) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta != null) {
            return meta.count;
        }
        return 0;
    }

    @Override
    public Optional<byte[]> mapGet(final byte[] key, final byte[] field) {
        final byte[] fullKey = Encoding.encodeDataMapFieldKey(getOrCreateKeyMeta(key, KeyType.Map).id, field);
        return Optional.ofNullable(dbGet(fullKey));
    }

    @Override
    public synchronized long mapPut(final byte[] key, final MapItem... items) {
        if (items.length > 0) {
            final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.Map);
            long newRows = 0;
            for (final MapItem item : items) {
                final byte[] fullKey = Encoding.encodeDataMapFieldKey(meta.id, item.field);
                if (dbGet(fullKey) == null) {
                    newRows++;
                }
                dbPut(fullKey, item.value);
            }
            if (newRows > 0) {
                meta.count += newRows;
                updateMetaInfo(key, meta);
            }
        }
        return items.length;
    }

    @Override
    public synchronized Optional<byte[]> mapRemove(final byte[] key, final byte[] field) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.Map);
        final byte[] fullKey = Encoding.encodeDataMapFieldKey(meta.id, field);
        final byte[] oldValue = dbGet(fullKey);
        if (oldValue != null) {
            meta.count--;
            dbDelete(fullKey);
            updateMetaInfo(key, meta);
        }
        return Optional.ofNullable(oldValue);
    }

    @Override
    public long mapForEach(final byte[] key, final Consumer<MapItem> onItem) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return 0;
        }
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> onItem.accept(MapItem.of(Encoding.stripDataKeyPrefix(entry.key()), entry.value())));
    }

    @Override
    public long mapCount(final byte[] key) {
        return getCount(key);
    }

    @Override
    public synchronized long listLeftPush(final byte[] key, final byte[]... values) {
        if (values.length > 0) {
            final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.List);
            final MetaInfo.ListExtra extra = MetaInfo.ListExtra.fromBytes(meta.extra);
            for (final byte[] value : values) {
                final byte[] fullKey = Encoding.encodeDataListKey(meta.id, extra.left--);
                dbPut(fullKey, value);
            }
            meta.count += values.length;
            meta.extra = extra.toBytes();
            updateMetaInfo(key, meta);
        }
        return values.length;
    }

    @Override
    public synchronized long listRightPush(final byte[] key, final byte[]... values) {
        if (values.length > 0) {
            final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.List);
            final MetaInfo.ListExtra extra = MetaInfo.ListExtra.fromBytes(meta.extra);
            for (final byte[] value : values) {
                final byte[] fullKey = Encoding.encodeDataListKey(meta.id, extra.right++);
                dbPut(fullKey, value);
            }
            meta.count += values.length;
            meta.extra = extra.toBytes();
            updateMetaInfo(key, meta);
        }
        return values.length;
    }

    @Override
    public long listCount(final byte[] key) {
        return getCount(key);
    }

    @Override
    public synchronized Optional<byte[]> listLeftPop(final byte[] key) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return Optional.empty();
        }
        final MetaInfo.ListExtra extra = MetaInfo.ListExtra.fromBytes(meta.extra);
        final byte[] fullKey = Encoding.encodeDataListKey(meta.id, ++extra.left);
        final byte[] value = dbGet(fullKey);
        if (value != null) {
            meta.count--;
            meta.extra = extra.toBytes();
            updateMetaInfo(key, meta);
            dbDelete(fullKey);
        }
        return Optional.ofNullable(value);
    }

    @Override
    public synchronized Optional<byte[]> listRightPop(final byte[] key) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return Optional.empty();
        }
        final MetaInfo.ListExtra extra = MetaInfo.ListExtra.fromBytes(meta.extra);
        final byte[] fullKey = Encoding.encodeDataListKey(meta.id, --extra.right);
        final byte[] value = dbGet(fullKey);
        if (value != null) {
            meta.count--;
            meta.extra = extra.toBytes();
            updateMetaInfo(key, meta);
            dbDelete(fullKey);
        }
        return Optional.ofNullable(value);
    }

    @Override
    public long listForEach(final byte[] key, final Consumer<ListItem> onItem) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return 0;
        }
        final Box<Long> index = Box.of(0L);
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> onItem.accept(ListItem.of(index.value++, entry.value())));
    }

    @Override
    public synchronized long setAdd(final byte[] key, final byte[]... values) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.Set);
        long newRows = 0;
        for (final byte[] value : values) {
            final byte[] fullKey = Encoding.encodeDataSetKey(meta.id, value);
            if (dbGet(fullKey) == null) {
                newRows++;
            }
            dbPut(fullKey, new byte[]{});
        }
        if (newRows > 0) {
            meta.count += newRows;
            updateMetaInfo(key, meta);
        }
        return newRows;
    }

    @Override
    public boolean setIsMember(final byte[] key, final byte[]... values) {
        if (values.length < 1) {
            return false;
        }
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return false;
        }
        boolean yes = true;
        for (final byte[] value : values) {
            final byte[] fullKey = Encoding.encodeDataSetKey(meta.id, value);
            yes &= dbGet(fullKey) != null;
        }
        return yes;
    }

    @Override
    public synchronized long setRemove(final byte[] key, final byte[]... values) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return 0;
        }
        long deleteRows = 0;
        for (final byte[] value : values) {
            final byte[] fullKey = Encoding.encodeDataSetKey(meta.id, value);
            if (dbGet(fullKey) != null) {
                deleteRows++;
                dbDelete(fullKey);
            }
        }
        if (deleteRows > 0) {
            meta.count -= deleteRows;
            updateMetaInfo(key, meta);
        }
        return deleteRows;
    }

    @Override
    public long setCount(final byte[] key) {
        return getCount(key);
    }

    @Override
    public long setForEach(final byte[] key, final Consumer<byte[]> onItem) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return 0;
        }
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> onItem.accept(Encoding.decodeDataSetKey(entry.key())));
    }

    @Override
    public synchronized long sortedListAdd(final byte[] key, final SortedListItem... items) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.SortedList);
        final MetaInfo.SortedListExtra extra = MetaInfo.SortedListExtra.fromBytes(meta.extra);
        for (final SortedListItem item : items) {
            final byte[] fullKey = Encoding.encodeDataSortedListKey(meta.id, extra.sequence++, item.score);
            dbPut(fullKey, item.value);
        }
        meta.extra = extra.toBytes();
        meta.count += items.length;
        updateMetaInfo(key, meta);
        return items.length;
    }

    @Override
    public long sortedListCount(final byte[] key) {
        return getCount(key);
    }

    protected void checkSortedListCompact(final MetaInfo.SortedListExtra extra) {
        try {
            if (extra.leftDeletesCount >= 300) {
                db.compactRange();
                extra.leftDeletesCount = 0;
            }
            if (extra.rightDeletesCount >= 300) {
                db.compactRange();
                extra.rightDeletesCount = 0;
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public synchronized Optional<SortedListItem> sortedListLeftPop(final byte[] key, final byte[] maxScore) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.SortedList);
        final MetaInfo.SortedListExtra extra = MetaInfo.SortedListExtra.fromBytes(meta.extra);
        final byte[] prefix = Encoding.encodeDataSortedListPrefixKey(meta.id);
        try (final ReadOptions readOptions = dbReadOptions(o -> {
            o.setPrefixSameAsStart(true);
            o.setTotalOrderSeek(false);
        })) {
            try (final RocksIterator it = dbIterator(readOptions)) {
                it.seek(prefix);
                if (!it.isValid()) {
                    return Optional.empty();
                }
                if (!Encoding.hasPrefix(prefix, it.key())) {
                    return Optional.empty();
                }
                final byte[] score = Encoding.decodeDataSortedListKey(it.key());
                if (maxScore == null || Encoding.compareScoreBytes(score, maxScore) < 1) {
                    dbDelete(it.key());
                    meta.count--;
                    extra.leftDeletesCount++;
                    checkSortedListCompact(extra);
                    meta.extra = extra.toBytes();
                    updateMetaInfo(key, meta);
                    return Optional.of(SortedListItem.of(score, it.value()));
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    @Override
    public synchronized Optional<SortedListItem> sortedListRightPop(final byte[] key, final byte[] minScore) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.SortedList);
        final MetaInfo.SortedListExtra extra = MetaInfo.SortedListExtra.fromBytes(meta.extra);
        final byte[] prefix = Encoding.encodeDataSortedListPrefixKey(meta.id);
        try (final ReadOptions readOptions = dbReadOptions(o -> {
            o.setPrefixSameAsStart(true);
            o.setTotalOrderSeek(false);
        })) {
            try (final RocksIterator it = dbIterator(readOptions)) {
                it.seekForPrev(Encoding.encodeDataSortedListPrefixKey(meta.id + 1));
                if (!it.isValid()) {
                    return Optional.empty();
                }
                if (!Encoding.hasPrefix(prefix, it.key())) {
                    return Optional.empty();
                }
                final byte[] score = Encoding.decodeDataSortedListKey(it.key());
                if (minScore == null || Encoding.compareScoreBytes(score, minScore) >= 0) {
                    dbDelete(it.key());
                    meta.count--;
                    extra.rightDeletesCount++;
                    checkSortedListCompact(extra);
                    meta.extra = extra.toBytes();
                    updateMetaInfo(key, meta);
                    return Optional.of(SortedListItem.of(score, it.value()));
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    @Override
    public long sortedListForEach(final byte[] key, final Consumer<SortedListItem> onItem) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return 0;
        }
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> onItem.accept(SortedListItem.of(Encoding.decodeDataSortedListKey(entry.key()), entry.value())));
    }

    @Override
    public synchronized long ascSortedListAdd(final byte[] key, final SortedListItem... items) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.AscSortedList);
        final MetaInfo.AscSortedListExtra extra = MetaInfo.AscSortedListExtra.fromBytes(meta.extra);
        long addCount = 0;
        for (final SortedListItem item : items) {
            final byte[] fullKey = Encoding.encodeDataSortedListKey(meta.id, extra.sequence++, item.score);
            if (extra.minKey == null || Encoding.compareScoreBytes(fullKey, extra.minKey) >= 0) {
                addCount++;
                dbPut(fullKey, item.value);
            }
        }
        meta.extra = extra.toBytes();
        meta.count += addCount;
        updateMetaInfo(key, meta);
        return addCount;
    }

    @Override
    public long ascSortedListCount(final byte[] key) {
        return getCount(key);
    }

    @Override
    public synchronized Optional<SortedListItem> ascSortedListPop(final byte[] key, final byte[] maxScore) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return Optional.empty();
        }
        final MetaInfo.AscSortedListExtra extra = MetaInfo.AscSortedListExtra.fromBytes(meta.extra);
        final byte[] prefix = Encoding.encodeDataSortedListPrefixKey(meta.id);
        final byte[] minKey = extra.minKey != null ? extra.minKey : prefix;
        try (final ReadOptions readOptions = dbReadOptions(null)) {
            try (final RocksIterator it = dbIterator(readOptions)) {
                it.seek(minKey);
                if (!it.isValid()) {
                    return Optional.empty();
                }
                if (!Encoding.hasPrefix(prefix, it.key())) {
                    return Optional.empty();
                }
                final byte[] score = Encoding.decodeDataSortedListKey(it.key());
                if (maxScore == null || Encoding.compareScoreBytes(score, maxScore) < 1) {
                    meta.count--;
                    extra.deletesCount++;
                    extra.minKey = Encoding.prefixUpperBound(it.key());
                    if (meta.count < 1) {
                        pruneAscSortedListRange(meta, extra);
                    } else if (extra.deletesCount >= 300) {
                        pruneAscSortedListRange(meta, extra);
                        extra.deletesCount = 0;
                    }
                    meta.extra = extra.toBytes();
                    updateMetaInfo(key, meta);
                    return Optional.of(SortedListItem.of(score, it.value()));
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    @Override
    public long ascSortedListForEach(final byte[] key, final Consumer<SortedListItem> onItem) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return 0;
        }
        final MetaInfo.AscSortedListExtra extra = MetaInfo.AscSortedListExtra.fromBytes(meta.extra);
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> {
            if (extra.minKey == null || Encoding.compareScoreBytes(entry.key(), extra.minKey) >= 0) {
                onItem.accept(SortedListItem.of(Encoding.decodeDataSortedListKey(entry.key()), entry.value()));
            }
        });
    }

    @Override
    public synchronized void ascSortedListPrune(byte[] key) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return;
        }
        final MetaInfo.AscSortedListExtra extra = MetaInfo.AscSortedListExtra.fromBytes(meta.extra);
        pruneAscSortedListRange(meta, extra);
    }

    protected void pruneAscSortedListRange(final MetaInfo meta, final MetaInfo.AscSortedListExtra extra) {
        if (extra.minKey == null) {
            return;
        }
        final byte[] prefix = Encoding.encodeDataSortedListPrefixKey(meta.id);
        try {
            db.deleteRange(prefix, extra.minKey);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public long forEachKeys(final byte[] prefix, BiConsumer<byte[], MetaInfo> onItem) {
        return prefixForEach(Encoding.combineMultipleBytes(Encoding.KEY_PREFIX_META, prefix), (entry -> {
            final MetaInfo meta = MetaInfo.fromBytes(entry.value());
            onItem.accept(Encoding.stripDataKeyPrefix(entry.key()), meta);
        }));
    }
}
