package com.leizm.cedar.core;

import org.iq80.leveldb.DBIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

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
            Database db = new Database(path);
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
            list2.add(key);
        });
        assertEquals(new HashSet<>().addAll(list), new HashSet<>().addAll(list2));
    }

    void testMapForKey(final Database db, final byte[] key) {
        System.out.println("testMapForKey: " + new String(key));

        assertEquals(Optional.empty(), db.mapGet(key, "a".getBytes()));

        assertEquals(1, db.mapPut(key, MapItem.of("a".getBytes(), "123".getBytes())));
        assertArrayEquals("123".getBytes(), db.mapGet(key, "a".getBytes()).get());
        assertEquals(1, db.mapSize(key));

        assertEquals(3, db.mapPut(key,
                MapItem.of("b".getBytes(), "qq".getBytes()),
                MapItem.of("c".getBytes(), "xx".getBytes()),
                MapItem.of("d".getBytes(), "zz".getBytes())
        ));
        assertArrayEquals("qq".getBytes(), db.mapGet(key, "b".getBytes()).get());
        assertArrayEquals("xx".getBytes(), db.mapGet(key, "c".getBytes()).get());
        assertArrayEquals("zz".getBytes(), db.mapGet(key, "d".getBytes()).get());

        final List<String> values = db.mapItems(key).stream().map(item -> String.format("%s=%s", new String(item.field), new String(item.value))).collect(Collectors.toList());
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
        final List<byte[]> list = generateRandomKeyList(10);
        list.forEach(key -> testSortedListForKey(db, key));

        // test forEachKeys
        final List<byte[]> list2 = new ArrayList<>();
        db.forEachKeys((key, meta) -> {
            list2.add(key);
        });
        assertEquals(new HashSet<>().addAll(list), new HashSet<>().addAll(list2));
    }

    void testSortedListForKey(final Database db, final byte[] key) {
        System.out.println("testSortedListForKey: " + new String(key));

        // test left
        assertEquals(0, db.sortedListSize(key));
        assertEquals(2, db.sortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(6), "aaa".getBytes()),
                SortedListItem.of(Encoding.longToBytes(5), "bbb".getBytes())
        ));
        assertEquals(2, db.sortedListSize(key));
        assertEquals("bbb", new String(db.sortedListLeftPop(key, null).get().value));
        assertEquals("aaa", new String(db.sortedListLeftPop(key, null).get().value));
        assertEquals(0, db.sortedListSize(key));
        assertEquals(3, db.sortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(2), "x".getBytes()),
                SortedListItem.of(Encoding.longToBytes(1), "y".getBytes()),
                SortedListItem.of(Encoding.longToBytes(1), "z".getBytes())
        ));
        assertEquals(3, db.sortedListSize(key));
        final List<SortedListItem> values = db.sortedListItems(key);
        assertEquals(3, values.size());
        assertEquals("y", new String(values.get(0).value));
        assertEquals("z", new String(values.get(1).value));
        assertEquals("x", new String(values.get(2).value));
        assertEquals(1, Encoding.longFromBytes(values.get(0).score));
        assertEquals(1, Encoding.longFromBytes(values.get(1).score));
        assertEquals(2, Encoding.longFromBytes(values.get(2).score));
        assertEquals(Optional.empty(), db.sortedListLeftPop(key, Encoding.longToBytes(0)));
        assertEquals("y", new String(db.sortedListLeftPop(key, Encoding.longToBytes(1)).get().value));
        assertEquals("z", new String(db.sortedListLeftPop(key, Encoding.longToBytes(1)).get().value));
        assertEquals(Optional.empty(), db.sortedListLeftPop(key, Encoding.longToBytes(1)));
        assertEquals("x", new String(db.sortedListLeftPop(key, Encoding.longToBytes(2)).get().value));
        assertEquals(Optional.empty(), db.sortedListLeftPop(key, Encoding.longToBytes(2)));

        // test right
        assertEquals(0, db.sortedListSize(key));
        assertEquals(2, db.sortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(6), "aaa".getBytes()),
                SortedListItem.of(Encoding.longToBytes(5), "bbb".getBytes())
        ));
        assertEquals(2, db.sortedListSize(key));
        assertEquals("aaa", new String(db.sortedListRightPop(key, null).get().value));
        assertEquals("bbb", new String(db.sortedListRightPop(key, null).get().value));
        assertEquals(0, db.sortedListSize(key));
        assertEquals(3, db.sortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(2), "x".getBytes()),
                SortedListItem.of(Encoding.longToBytes(1), "y".getBytes()),
                SortedListItem.of(Encoding.longToBytes(1), "z".getBytes())
        ));
        assertEquals(Optional.empty(), db.sortedListRightPop(key, Encoding.longToBytes(5)));
        assertEquals("x", new String(db.sortedListRightPop(key, Encoding.longToBytes(2)).get().value));
        assertEquals(Optional.empty(), db.sortedListRightPop(key, Encoding.longToBytes(2)));
        assertEquals("z", new String(db.sortedListRightPop(key, Encoding.longToBytes(1)).get().value));
        assertEquals("y", new String(db.sortedListRightPop(key, Encoding.longToBytes(1)).get().value));
        assertEquals(Optional.empty(), db.sortedListRightPop(key, Encoding.longToBytes(1)));
        assertEquals(0, db.sortedListSize(key));
    }

    @Test
    void testList() {
        final Database db = createTempDatabase();
        final List<byte[]> list = generateRandomKeyList(10);
        list.forEach(key -> testListForKey(db, key));

        // test forEachKeys
        final List<byte[]> list2 = new ArrayList<>();
        db.forEachKeys((key, meta) -> {
            list2.add(key);
        });
        assertEquals(new HashSet<>().addAll(list), new HashSet<>().addAll(list2));
    }

    void testListForKey(final Database db, final byte[] key) {
        System.out.println("testListForKey: " + new String(key));

        assertEquals(0, db.listSize(key));
        assertEquals(2, db.listLeftPush(key, "a".getBytes(), "b".getBytes()));
        assertEquals(2, db.listSize(key));
        assertEquals(Arrays.asList("0=b", "1=a"),
                db.listItems(key).stream().map(item -> String.format("%d=%s", item.index, new String(item.value))).collect(Collectors.toList()));

        assertEquals(3, db.listRightPush(key, "c".getBytes(), "d".getBytes(), "e".getBytes()));
        assertEquals(5, db.listSize(key));
        assertEquals(Arrays.asList("0=b", "1=a", "2=c", "3=d", "4=e"),
                db.listItems(key).stream().map(item -> String.format("%d=%s", item.index, new String(item.value))).collect(Collectors.toList()));

        final List<String> values = new ArrayList<>();
        values.add(new String(db.listLeftPop(key).get()));
        values.add(new String(db.listRightPop(key).get()));
        values.add(new String(db.listLeftPop(key).get()));
        values.add(new String(db.listRightPop(key).get()));
        values.add(new String(db.listLeftPop(key).get()));
        assertEquals(Optional.empty(), db.listLeftPop(key));
        assertEquals(Optional.empty(), db.listRightPop(key));
        assertEquals(Arrays.asList("b", "e", "a", "d", "c"), values);

        assertEquals(0, db.listSize(key));
        assertEquals(2, db.listRightPush(key, "a".getBytes(), "b".getBytes()));
        assertEquals(2, db.listSize(key));
        assertEquals(Arrays.asList("0=a", "1=b"),
                db.listItems(key).stream().map(item -> String.format("%d=%s", item.index, new String(item.value))).collect(Collectors.toList()));
    }
}
