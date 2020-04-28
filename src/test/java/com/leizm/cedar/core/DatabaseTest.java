package com.leizm.cedar.core;

import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;


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

    static List<byte[]> generateRandomKeyList(int count) {
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(String.format("key-%d-%d", System.currentTimeMillis(), i).getBytes());
        }
        return list;
    }

    @Test
    void testMap() {
        final Database db = createTempDatabase();
        final List<byte[]> list = generateRandomKeyList(10);
        list.forEach(key -> testMapForKey(db, key));

        // test forEachKeys
        final List<byte[]> list2 = new ArrayList<>();
        db.forEachKeys((key, meta) -> {
            assertEquals(3, meta.size);
            list2.add(key);
        });
        assertEquals(new HashSet<>().addAll(list), new HashSet<>().addAll(list2));
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

    @Test
    void testSet() {
        final Database db = createTempDatabase();
        final List<byte[]> list = generateRandomKeyList(10);
        list.forEach(key -> testSetForKey(db, key));

        // test forEachKeys
        final List<byte[]> list2 = new ArrayList<>();
        db.forEachKeys((key, meta) -> {
            assertEquals(2, meta.size);
            list2.add(key);
        });
        assertEquals(new HashSet<>().addAll(list), new HashSet<>().addAll(list2));
    }

    void testSetForKey(final Database db, final byte[] key) {
        System.out.println("testSetForKey: " + new String(key));

        assertFalse(db.setIsMember(key, "a".getBytes()));

        assertEquals(2, db.setAdd(key, "a".getBytes(), "b".getBytes()));
        assertTrue(db.setIsMember(key, "a".getBytes()));
        assertTrue(db.setIsMember(key, "b".getBytes()));
        assertFalse(db.setIsMember(key, "c".getBytes()));
        assertEquals(2, db.setSize(key));

        assertEquals(1, db.setAdd(key, "a".getBytes(), "b".getBytes(), "c".getBytes()));
        assertTrue(db.setIsMember(key, "c".getBytes()));
        assertEquals(3, db.setSize(key));

        assertEquals(1, db.setRemove(key, "x".getBytes(), "c".getBytes()));
        assertEquals(2, db.setSize(key));

        final Object[] list = db.setMembers(key).stream().map(String::new).toArray();
        assertArrayEquals(new String[]{"a", "b"}, list);
    }

    @Test
    void testSortedList() {
        final Database db = createTempDatabase();
        final List<byte[]> list = generateRandomKeyList(1);
        list.forEach(key -> testSortedListForKey(db, key));

        // test forEachKeys
        // final List<byte[]> list2 = new ArrayList<>();
        // db.forEachKeys((key, meta) -> {
        //     assertEquals(3, meta.size);
        //     list2.add(key);
        // });
        // assertEquals(new HashSet<>().addAll(list), new HashSet<>().addAll(list2));
    }

    void testSortedListForKey(final Database db, final byte[] key) {
        System.out.println("testSortedListForKey: " + new String(key));

        assertEquals(0, db.sortedListSize(key));
        assertEquals(2, db.sortedListAdd(key,
                Encoding.longToBytes(6), "aaa".getBytes(),
                Encoding.longToBytes(5), "bbb".getBytes()
        ));
        assertEquals(2, db.sortedListSize(key));
        assertArrayEquals("bbb".getBytes(), db.sortedListLeftPop(key, null).get());
        assertArrayEquals("aaa".getBytes(), db.sortedListLeftPop(key, null).get());
        assertEquals(0, db.sortedListSize(key));

        assertEquals(3, db.sortedListAdd(key,
                Encoding.longToBytes(2), "x".getBytes(),
                Encoding.longToBytes(1), "y".getBytes(),
                Encoding.longToBytes(1), "z".getBytes()
        ));
        assertEquals(3, db.sortedListSize(key));
        assertEquals(Optional.empty(), db.sortedListLeftPop(key, Encoding.longToBytes(0)));
    }
}