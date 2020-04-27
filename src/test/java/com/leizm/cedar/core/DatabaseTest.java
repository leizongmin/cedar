package com.leizm.cedar.core;

import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


class DatabaseTest {

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    static List<Database> dbList = new ArrayList<>();

    static Database createTempDatabase() {
        String path = Paths.get(
                System.getProperty("java.io.tmpdir"),
                String.format("cedar-test-%d-%d", dbList.size(), System.currentTimeMillis())
        ).toAbsolutePath().toString();
        return createTempDatabase(path);
    }

    static Database createTempDatabase(String path) {
        try {
            System.out.printf("create database on path: %s\n", path);
            Database db = new Database(path, new Options().createIfMissing(true));
            dbList.add(db);
            return db;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    static void dumpDatabase(Database db) {
        System.out.println("==================== dumpDatabase ====================");
        System.out.println("path: " + db.getPath());
        DBIterator iter = db.getDb().iterator();
        iter.seekToFirst();
        while (iter.hasNext()) {
            final Map.Entry<byte[], byte[]> entry = iter.next();
            System.out.printf("%s (%s) = %s (%s) \n",
                    bytesToHex(entry.getKey()), new String(entry.getKey()),
                    bytesToHex(entry.getValue()), new String(entry.getValue()));
        }
        System.out.println();
        try {
            iter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void cleanup() {
        dbList.forEach(db -> {
            try {
                dumpDatabase(db);
                db.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    static List<byte[]> generateRandomKeyList() {
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(String.format("key-%d-%d", System.currentTimeMillis(), i).getBytes());
        }
        return list;
    }

    @Test
    void testMap() {
        final Database db = createTempDatabase();
        final List<byte[]> list = generateRandomKeyList();
        list.forEach(key -> testMapForKey(db, key));
    }

    void testMapForKey(final Database db, final byte[] key) {
        System.out.println("testMapForKey: " + new String(key));

        assertEquals(Optional.empty(), db.mapGet(key, "a".getBytes()));

        assertEquals(1, db.mapPut(key, "a".getBytes(), "123".getBytes()));
        assertArrayEquals("123".getBytes(), db.mapGet(key, "a".getBytes()).get());
        assertEquals(1, db.mapSize(key));

        assertEquals(3, db.mapPut(key,
                "b".getBytes(), "qq".getBytes(),
                "c".getBytes(), "xx".getBytes(),
                "d".getBytes(), "zz".getBytes()
        ));
        assertArrayEquals("qq".getBytes(), db.mapGet(key, "b".getBytes()).get());
        assertArrayEquals("xx".getBytes(), db.mapGet(key, "c".getBytes()).get());
        assertArrayEquals("zz".getBytes(), db.mapGet(key, "d".getBytes()).get());

        final List<String> values = new ArrayList<>();
        db.mapForEach(key, (k, v) -> {
            values.add(String.format("%s=%s", new String(k), new String(v)));
        });
        assertEquals(Arrays.asList("a=123", "b=qq", "c=xx", "d=zz"), values);
        assertEquals(4, db.mapSize(key));

        assertEquals(Optional.empty(), db.mapRemove(key, "xzz".getBytes()));
        assertArrayEquals("123".getBytes(), db.mapRemove(key, "a".getBytes()).get());
        assertEquals(Optional.empty(), db.mapRemove(key, "a".getBytes()));
        assertEquals(3, db.mapSize(key));
    }
}