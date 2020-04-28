package com.leizm.cedar.core;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.fusesource.leveldbjni.JniDBFactory.factory;


public class Database implements IDatabase {
    /**
     * LevelDB database instance
     */
    protected DB db;

    /**
     * database path
     */
    protected String path;

    /**
     * next key id
     */
    protected long nextKeyId = 0;

    /**
     * open database
     *
     * @param path    store path
     * @param options LevelDB options
     * @throws IOException
     */
    public Database(String path, Options options) throws IOException {
        db = factory.open(Paths.get(path).toFile(), options);
        this.path = path;
        initAfterOpen();
    }

    /**
     * returns LevelDB instance
     *
     * @return
     */
    public DB getDb() {
        return db;
    }

    /**
     * returns database path
     *
     * @return
     */
    public String getPath() {
        return path;
    }

    /**
     * close database
     *
     * @throws IOException
     */
    public void close() throws IOException {
        db.close();
    }

    protected void initAfterOpen() throws IOException {
        final Box<Long> maxKeyId = Box.of(1L);
        prefixForEach(Encoding.KEY_PREFIX_META, (entry -> {
            final MetaInfo meta = MetaInfo.fromBytes(entry.getValue());
            if (meta.id > maxKeyId.value) {
                maxKeyId.value = meta.id;
            }
        }));
        nextKeyId = maxKeyId.value + 1;
    }

    protected long prefixForEach(final byte[] prefix, final Consumer<Map.Entry<byte[], byte[]>> onItem) {
        DBIterator iter = dbIter();
        long count = 0;
        try {
            iter.seek(prefix);
            while (iter.hasNext()) {
                final Map.Entry<byte[], byte[]> entry = iter.next();
                if (!Encoding.isSamePrefix(prefix, entry.getKey())) {
                    break;
                }
                onItem.accept(entry);
                count++;
            }
        } finally {
            try {
                iter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return count;
    }

    protected DBIterator dbIter() {
        return db.iterator();
    }

    protected byte[] dbGet(byte[] key) {
        // System.out.printf("GET %s\n", new String(key));
        try {
            return db.get(key);
        } catch (DBException e) {
            return null;
        }
    }

    protected void dbPut(byte[] key, byte[] value) {
        // System.out.printf("PUT %s = %s\n", new String(key), new String(value));
        db.put(key, value);
    }

    protected void dbDelete(byte[] key) {
        // System.out.printf("DELETE %s\n", new String(key));
        db.delete(key);
    }

    protected MetaInfo getKeyMeta(byte[] key) {
        final byte[] fullKey = Encoding.encodeMetaKey(key);
        return MetaInfo.fromBytes(dbGet(fullKey));
    }

    protected MetaInfo getOrCreateKeyMeta(byte[] key, KeyType type) {
        final byte[] fullKey = Encoding.encodeMetaKey(key);
        MetaInfo meta = MetaInfo.fromBytes(dbGet(fullKey));
        if (meta == null) {
            meta = new MetaInfo(nextKeyId++, type, 0, null);
            dbPut(fullKey, meta.toBytes());
        } else if (!meta.type.equals(type)) {
            throw new IllegalArgumentException(String.format("expected type %s but actually %s", type.name(), meta.type.name()));
        }
        return meta;
    }

    protected void updateMetaInfo(byte[] key, MetaInfo meta) {
        dbPut(Encoding.encodeMetaKey(key), meta.toBytes());
    }

    protected void checkPairsArgument(final byte[]... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException(String.format("pairs length is %d", pairs.length));
        }
    }

    protected long getSize(final byte[] key) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta != null) {
            return meta.size;
        }
        return 0;
    }

    @Override
    public Optional<byte[]> mapGet(final byte[] key, final byte[] field) {
        final byte[] fullKey = Encoding.encodeDataMapFieldKey(getOrCreateKeyMeta(key, KeyType.Map).id, field);
        return Optional.ofNullable(dbGet(fullKey));
    }

    @Override
    public synchronized long mapPut(final byte[] key, final byte[]... pairs) {
        checkPairsArgument(pairs);
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.Map);
        long newRows = 0;
        for (int i = 0; i < pairs.length; i += 2) {
            final byte[] fullKey = Encoding.encodeDataMapFieldKey(meta.id, pairs[i]);
            if (dbGet(fullKey) == null) {
                newRows++;
            }
            dbPut(fullKey, pairs[i + 1]);
        }
        if (newRows > 0) {
            meta.size += newRows;
            updateMetaInfo(key, meta);
        }
        return newRows;
    }

    @Override
    public synchronized Optional<byte[]> mapRemove(final byte[] key, final byte[] field) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.Map);
        final byte[] fullKey = Encoding.encodeDataMapFieldKey(meta.id, field);
        final byte[] oldValue = dbGet(fullKey);
        if (oldValue != null) {
            meta.size--;
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
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> {
            onItem.accept(MapItem.of(Encoding.stripDataKeyPrefix(entry.getKey()), entry.getValue()));
        });
    }

    @Override
    public long mapSize(final byte[] key) {
        return getSize(key);
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
            meta.size += values.length;
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
            meta.size += values.length;
            meta.extra = extra.toBytes();
            updateMetaInfo(key, meta);
        }
        return values.length;
    }

    @Override
    public long listSize(final byte[] key) {
        return getSize(key);
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
            meta.size--;
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
            meta.size--;
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
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> {
            onItem.accept(ListItem.of(index.value++, entry.getValue()));
        });
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
            meta.size += newRows;
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
            meta.size -= deleteRows;
            updateMetaInfo(key, meta);
        }
        return deleteRows;
    }

    @Override
    public long setSize(final byte[] key) {
        return getSize(key);
    }

    @Override
    public long setForEach(final byte[] key, final Consumer<byte[]> onItem) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return 0;
        }
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> {
            onItem.accept(Encoding.decodeDataSetKey(entry.getKey()));
        });
    }

    @Override
    public synchronized long sortedListAdd(final byte[] key, final SortedListItem... items) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.SortedList);
        long seq = meta.extra == null ? 1 : Encoding.longFromBytes(meta.extra);
        for (int i = 0; i < items.length; i++) {
            final byte[] fullKey = Encoding.encodeDataSortedListKey(meta.id, seq++, items[i].score);
            dbPut(fullKey, items[i].value);
        }
        meta.extra = Encoding.longToBytes(seq);
        meta.size += items.length;
        updateMetaInfo(key, meta);
        return items.length;
    }

    @Override
    public long sortedListSize(final byte[] key) {
        return getSize(key);
    }

    @Override
    public synchronized Optional<SortedListItem> sortedListLeftPop(final byte[] key, final byte[] maxScore) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.SortedList);
        final byte[] prefix = Encoding.encodeDataSortedListPrefixKey(meta.id);
        final DBIterator iter = dbIter();
        try {
            iter.seek(prefix);
            if (!iter.hasNext()) {
                return Optional.empty();
            }
            final Map.Entry<byte[], byte[]> first = iter.next();
            if (!Encoding.isSamePrefix(prefix, first.getKey())) {
                return Optional.empty();
            }
            final byte[] score = Encoding.decodeDataSortedListKey(first.getKey());
            if (maxScore == null || Encoding.compareScoreBytes(score, maxScore) < 1) {
                dbDelete(first.getKey());
                meta.size--;
                updateMetaInfo(key, meta);
                return Optional.of(SortedListItem.of(score, first.getValue()));
            } else {
                return Optional.empty();
            }
        } finally {
            try {
                iter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public synchronized Optional<SortedListItem> sortedListRightPop(final byte[] key, final byte[] minScore) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.SortedList);
        final byte[] prefix = Encoding.encodeDataSortedListPrefixKey(meta.id);
        final DBIterator iter = dbIter();
        try {
            iter.seek(Encoding.encodeDataSortedListPrefixKey(meta.id + 1));
            if (!iter.hasPrev()) {
                return Optional.empty();
            }
            final Map.Entry<byte[], byte[]> last = iter.prev();
            if (!Encoding.isSamePrefix(prefix, last.getKey())) {
                return Optional.empty();
            }
            final byte[] score = Encoding.decodeDataSortedListKey(last.getKey());
            if (minScore == null || Encoding.compareScoreBytes(score, minScore) >= 0) {
                dbDelete(last.getKey());
                meta.size--;
                updateMetaInfo(key, meta);
                return Optional.of(SortedListItem.of(score, last.getValue()));
            } else {
                return Optional.empty();
            }
        } finally {
            try {
                iter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public long sortedListForEach(final byte[] key, final Consumer<SortedListItem> onItem) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta == null) {
            return 0;
        }
        return prefixForEach(Encoding.encodeDataMapPrefixKey(meta.id), entry -> {
            onItem.accept(SortedListItem.of(Encoding.decodeDataSortedListKey(entry.getKey()), entry.getValue()));
        });
    }

    @Override
    public long forEachKeys(final byte[] prefix, BiConsumer<byte[], MetaInfo> onItem) {
        return prefixForEach(Encoding.combineMultipleBytes(Encoding.KEY_PREFIX_META, prefix), (entry -> {
            final MetaInfo meta = MetaInfo.fromBytes(entry.getValue());
            onItem.accept(Encoding.stripDataKeyPrefix(entry.getKey()), meta);
        }));
    }
}
