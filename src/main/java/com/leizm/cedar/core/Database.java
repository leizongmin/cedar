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
     * next ObjectId
     */
    protected long nextObjectId = 0;

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
        final Box<Long> maxObjectId = new Box<>(1L);
        prefixForEach(Encoding.KEYPREFIX_META, (entry -> {
            final MetaInfo meta = MetaInfo.fromBytes(entry.getValue());
            if (meta.objectId > maxObjectId.value) {
                maxObjectId.value = meta.objectId;
            }
        }));
        nextObjectId = maxObjectId.value + 1;
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
            meta = new MetaInfo(nextObjectId++, type, 0);
            dbPut(fullKey, meta.toBytes());
        } else if (!meta.type.equals(type)) {
            throw new IllegalArgumentException(String.format("expected type %s but actually %s", type.name(), meta.type.name()));
        }
        return meta;
    }

    protected void updateMetaInfo(byte[] key, MetaInfo meta) {
        dbPut(Encoding.encodeMetaKey(key), meta.toBytes());
    }

    @Override
    public Optional<byte[]> mapGet(final byte[] key, final byte[] field) {
        final byte[] fullKey = Encoding.encodeDataMapFieldKey(getOrCreateKeyMeta(key, KeyType.Map).objectId, field);
        return Optional.ofNullable(dbGet(fullKey));
    }

    @Override
    public synchronized long mapPut(final byte[] key, final byte[]... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException(String.format("pairs length is %d", pairs.length));
        }
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.Map);
        long newRows = 0;
        for (int i = 0; i < pairs.length; i += 2) {
            final byte[] fullKey = Encoding.encodeDataMapFieldKey(meta.objectId, pairs[i]);
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
        final byte[] fullKey = Encoding.encodeDataMapFieldKey(meta.objectId, field);
        final byte[] oldValue = dbGet(fullKey);
        if (oldValue != null) {
            meta.size--;
            dbDelete(fullKey);
            updateMetaInfo(key, meta);
        }
        return Optional.ofNullable(oldValue);
    }

    @Override
    public long mapForEach(final byte[] key, final BiConsumer<byte[], byte[]> onItem) {
        return prefixForEach(Encoding.encodeDataMapPrefixKey(getOrCreateKeyMeta(key, KeyType.Map).objectId), entry -> {
            onItem.accept(Encoding.stripDataKeyPrefix(entry.getKey()), entry.getValue());
        });
    }

    @Override
    public long mapSize(final byte[] key) {
        final MetaInfo meta = getKeyMeta(key);
        if (meta != null) {
            return meta.size;
        }
        return 0;
    }

    @Override
    public synchronized long listLeftPush(final byte[] key, final byte[]... values) {
        return 0;
    }

    @Override
    public synchronized long listRightPush(final byte[] key, final byte[]... values) {
        return 0;
    }

    @Override
    public long listSize(final byte[] key) {
        return 0;
    }

    @Override
    public synchronized Optional<byte[]> listLeftPop(final byte[] key) {
        return Optional.empty();
    }

    @Override
    public synchronized Optional<byte[]> listRightPop(final byte[] key) {
        return Optional.empty();
    }

    @Override
    public long listForEach(final byte[] key, final Consumer<byte[]> onItem) {
        return 0;
    }

    @Override
    public synchronized long setAdd(final byte[] key, final byte[]... values) {
        final MetaInfo meta = getOrCreateKeyMeta(key, KeyType.Set);
        long newRows = 0;
        for (final byte[] value : values) {
            final byte[] fullKey = Encoding.encodeDataSetKey(meta.objectId, value);
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
            final byte[] fullKey = Encoding.encodeDataSetKey(meta.objectId, value);
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
            final byte[] fullKey = Encoding.encodeDataSetKey(meta.objectId, value);
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
        final MetaInfo meta = getKeyMeta(key);
        if (meta != null) {
            return meta.size;
        }
        return 0;
    }

    @Override
    public long setForEach(final byte[] key, final Consumer<byte[]> onItem) {
        return prefixForEach(Encoding.encodeDataMapPrefixKey(getOrCreateKeyMeta(key, KeyType.Set).objectId), entry -> {
            onItem.accept(Encoding.decodeDataSetKey(entry.getKey()));
        });
    }

    @Override
    public synchronized long sortedListAdd(final byte[] key, final byte[]... scoreValuePairs) {
        return 0;
    }

    @Override
    public long sortedListSize(final byte[] key) {
        return 0;
    }

    @Override
    public synchronized Optional<byte[]> sortedListLeftPop(final byte[] key, final byte[] maxScore) {
        return Optional.empty();
    }

    @Override
    public synchronized Optional<byte[]> sortedListRightPop(final byte[] key, final byte[] minScore) {
        return Optional.empty();
    }

    @Override
    public long sortedListForEach(final byte[] key, final Consumer<byte[]> onItem) {
        return 0;
    }

    @Override
    public long forEachKeys(final byte[] prefix, BiConsumer<byte[], MetaInfo> onItem) {
        return prefixForEach(Encoding.combineMultipleBytes(Encoding.KEYPREFIX_META, prefix), (entry -> {
            final MetaInfo meta = MetaInfo.fromBytes(entry.getValue());
            onItem.accept(Encoding.stripDataKeyPrefix(entry.getKey()), meta);
        }));
    }
}
