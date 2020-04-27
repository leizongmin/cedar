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

    protected MetaInfo getOrCreateKeyMeta(byte[] key) {
        final byte[] fullKey = Encoding.encodeMetaKey(key);
        MetaInfo meta = MetaInfo.fromBytes(dbGet(fullKey));
        if (meta == null) {
            meta = new MetaInfo(nextObjectId++, 0);
            dbPut(fullKey, meta.toBytes());
        }
        return meta;
    }

    protected long getOrCreateKeyObjectId(byte[] key) {
        final byte[] fullKey = Encoding.encodeMetaKey(key);
        byte[] ret = dbGet(fullKey);
        if (ret != null) {
            return Encoding.longFromBytes(ret);
        }
        MetaInfo meta = new MetaInfo(nextObjectId++, 0);
        dbPut(fullKey, meta.toBytes());
        return meta.objectId;
    }

    protected void updateMetaInfo(byte[] key, MetaInfo meta) {
        dbPut(Encoding.encodeMetaKey(key), meta.toBytes());
    }

    @Override
    public Optional<byte[]> mapGet(final byte[] key, final byte[] field) {
        final byte[] fullKey = Encoding.encodeDataMapFieldKey(getOrCreateKeyObjectId(key), field);
        return Optional.ofNullable(dbGet(fullKey));
    }

    @Override
    public long mapPut(final byte[] key, final byte[]... pairs) {
        if (pairs.length % 2 != 0) {
            throw new IllegalArgumentException(String.format("pairs length is %d", pairs.length));
        }
        final MetaInfo meta = getOrCreateKeyMeta(key);
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
    public Optional<byte[]> mapRemove(final byte[] key, final byte[] field) {
        final MetaInfo meta = getOrCreateKeyMeta(key);
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
        return prefixForEach(Encoding.encodeDataMapPrefixKey(getOrCreateKeyObjectId(key)), entry -> {
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
    public long listLeftPush(final byte[] key, final byte[]... values) {
        return 0;
    }

    @Override
    public long listRightPush(final byte[] key, final byte[]... values) {
        return 0;
    }

    @Override
    public long listSize(final byte[] key) {
        return 0;
    }

    @Override
    public Optional<byte[]> listLeftPop(final byte[] key) {
        return Optional.empty();
    }

    @Override
    public Optional<byte[]> listRightPop(final byte[] key) {
        return Optional.empty();
    }

    @Override
    public long listForEach(final byte[] key, final Consumer<byte[]> onItem) {
        return 0;
    }

    @Override
    public long setAdd(final byte[] key, final byte[]... values) {
        return 0;
    }

    @Override
    public boolean setIsMember(final byte[] key, final byte[]... value) {
        return false;
    }

    @Override
    public boolean setRemove(final byte[] key, final byte[]... values) {
        return false;
    }

    @Override
    public long setSize(final byte[] key) {
        return 0;
    }

    @Override
    public long setForEach(final byte[] key, final Consumer<byte[]> onItem) {
        return 0;
    }

    @Override
    public long sortedListAdd(final byte[] key, final byte[]... scoreValuePairs) {
        return 0;
    }

    @Override
    public long sortedListSize(final byte[] key) {
        return 0;
    }

    @Override
    public Optional<byte[]> sortedListLeftPop(final byte[] key, final byte[] maxScore) {
        return Optional.empty();
    }

    @Override
    public Optional<byte[]> sortedListRightPop(final byte[] key, final byte[] minScore) {
        return Optional.empty();
    }

    @Override
    public long sortedListForEach(final byte[] key, final Consumer<byte[]> onItem) {
        return 0;
    }

    @Override
    public long forEachKeys(final byte[] prefix) {
        return 0;
    }
}
