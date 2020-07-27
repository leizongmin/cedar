package com.leizm.cedar.core;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {
    @AfterAll
    static void cleanup() {
        TestUtil.dbList.forEach(db -> {
            TestUtil.dumpDatabase(db);
            db.close();
        });
    }

    @Test
    void testMap() {
        final Database db = TestUtil.createTempDatabase();
        final List<byte[]> list = TestUtil.generateRandomKeyList(10);
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
        assertEquals(1, db.mapCount(key));

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
        assertEquals(4, db.mapCount(key));

        assertEquals(Optional.empty(), db.mapRemove(key, "xzz".getBytes()));
        assertArrayEquals("123".getBytes(), db.mapRemove(key, "a".getBytes()).get());
        assertEquals(Optional.empty(), db.mapRemove(key, "a".getBytes()));
        assertEquals(3, db.mapCount(key));
    }

    @Test
    void testSet() {
        final Database db = TestUtil.createTempDatabase();
        final List<byte[]> list = TestUtil.generateRandomKeyList(10);
        list.forEach(key -> testSetForKey(db, key));

        // test forEachKeys
        final List<byte[]> list2 = new ArrayList<>();
        db.forEachKeys((key, meta) -> {
            assertEquals(2, meta.count);
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
        assertEquals(2, db.setCount(key));

        assertEquals(1, db.setAdd(key, "a".getBytes(), "b".getBytes(), "c".getBytes()));
        assertTrue(db.setIsMember(key, "c".getBytes()));
        assertEquals(3, db.setCount(key));

        assertEquals(1, db.setRemove(key, "x".getBytes(), "c".getBytes()));
        assertEquals(2, db.setCount(key));

        final Object[] list = db.setMembers(key).stream().map(String::new).toArray();
        assertArrayEquals(new String[]{"a", "b"}, list);
    }

    @Test
    void testSortedList() {
        final Database db = TestUtil.createTempDatabase();
        final List<byte[]> list = TestUtil.generateRandomKeyList(10);
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
        assertEquals(0, db.sortedListCount(key));
        assertEquals(2, db.sortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(6), "aaa".getBytes()),
                SortedListItem.of(Encoding.longToBytes(5), "bbb".getBytes())
        ));
        assertEquals(2, db.sortedListCount(key));
        assertEquals("bbb", new String(db.sortedListLeftPop(key, null).get().value));
        assertEquals("aaa", new String(db.sortedListLeftPop(key, null).get().value));
        assertEquals(0, db.sortedListCount(key));
        assertEquals(3, db.sortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(2), "x".getBytes()),
                SortedListItem.of(Encoding.longToBytes(1), "y".getBytes()),
                SortedListItem.of(Encoding.longToBytes(1), "z".getBytes())
        ));
        assertEquals(3, db.sortedListCount(key));
        final List<SortedListItem> items = db.sortedListItems(key);
        assertEquals(3, items.size());
        assertEquals("y", new String(items.get(0).value));
        assertEquals("z", new String(items.get(1).value));
        assertEquals("x", new String(items.get(2).value));
        assertEquals(1, Encoding.longFromBytes(items.get(0).score));
        assertEquals(1, Encoding.longFromBytes(items.get(1).score));
        assertEquals(2, Encoding.longFromBytes(items.get(2).score));
        assertEquals(Optional.empty(), db.sortedListLeftPop(key, Encoding.longToBytes(0)));
        assertEquals("y", new String(db.sortedListLeftPop(key, Encoding.longToBytes(1)).get().value));
        assertEquals("z", new String(db.sortedListLeftPop(key, Encoding.longToBytes(1)).get().value));
        assertEquals(Optional.empty(), db.sortedListLeftPop(key, Encoding.longToBytes(1)));
        assertEquals("x", new String(db.sortedListLeftPop(key, Encoding.longToBytes(2)).get().value));
        assertEquals(Optional.empty(), db.sortedListLeftPop(key, Encoding.longToBytes(2)));

        // test right
        assertEquals(0, db.sortedListCount(key));
        assertEquals(2, db.sortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(6), "aaa".getBytes()),
                SortedListItem.of(Encoding.longToBytes(5), "bbb".getBytes())
        ));
        assertEquals(2, db.sortedListCount(key));
        assertEquals("aaa", new String(db.sortedListRightPop(key, null).get().value));
        assertEquals("bbb", new String(db.sortedListRightPop(key, null).get().value));
        assertEquals(0, db.sortedListCount(key));
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
        assertEquals(0, db.sortedListCount(key));
    }

    @Test
    void testList() {
        final Database db = TestUtil.createTempDatabase();
        final List<byte[]> list = TestUtil.generateRandomKeyList(10);
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

        assertEquals(0, db.listCount(key));
        assertEquals(2, db.listLeftPush(key, "a".getBytes(), "b".getBytes()));
        assertEquals(2, db.listCount(key));
        assertEquals(Arrays.asList("0=b", "1=a"),
                db.listItems(key).stream().map(item -> String.format("%d=%s", item.index, new String(item.value))).collect(Collectors.toList()));

        assertEquals(3, db.listRightPush(key, "c".getBytes(), "d".getBytes(), "e".getBytes()));
        assertEquals(5, db.listCount(key));
        assertEquals(Arrays.asList("0=b", "1=a", "2=c", "3=d", "4=e"),
                db.listItems(key).stream().map(item -> String.format("%d=%s", item.index, new String(item.value))).collect(Collectors.toList()));

        final List<String> items = new ArrayList<>();
        items.add(new String(db.listLeftPop(key).get()));
        items.add(new String(db.listRightPop(key).get()));
        items.add(new String(db.listLeftPop(key).get()));
        items.add(new String(db.listRightPop(key).get()));
        items.add(new String(db.listLeftPop(key).get()));
        assertEquals(Optional.empty(), db.listLeftPop(key));
        assertEquals(Optional.empty(), db.listRightPop(key));
        assertEquals(Arrays.asList("b", "e", "a", "d", "c"), items);

        assertEquals(0, db.listCount(key));
        assertEquals(2, db.listRightPush(key, "a".getBytes(), "b".getBytes()));
        assertEquals(2, db.listCount(key));
        assertEquals(Arrays.asList("0=a", "1=b"),
                db.listItems(key).stream().map(item -> String.format("%d=%s", item.index, new String(item.value))).collect(Collectors.toList()));
    }

    @Test
    void testAscSortedList() {
        final Database db = TestUtil.createTempDatabase();
        final List<byte[]> list = TestUtil.generateRandomKeyList(1);
        list.forEach(key -> testAscSortedListForKey(db, key));

        // test forEachKeys
        final List<byte[]> list2 = new ArrayList<>();
        db.forEachKeys((key, meta) -> {
            list2.add(key);
        });
        assertEquals(new HashSet<>().addAll(list), new HashSet<>().addAll(list2));
    }

    void testAscSortedListForKey(final Database db, final byte[] key) {
        System.out.println("testAscSortedListForKey: " + new String(key));

        assertEquals(0, db.ascSortedListCount(key));
        assertEquals(2, db.ascSortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(6), "aaa".getBytes()),
                SortedListItem.of(Encoding.longToBytes(5), "bbb".getBytes())
        ));
        assertEquals(2, db.ascSortedListCount(key));

        assertEquals(Optional.empty(), db.ascSortedListPop(key, Encoding.longToBytes(0)));
        assertEquals(Optional.empty(), db.ascSortedListPop(key, Encoding.longToBytes(1)));
        assertEquals("bbb", new String(db.ascSortedListPop(key, null).get().value));
        assertEquals("aaa", new String(db.ascSortedListPop(key, Encoding.longToBytes(6)).get().value));
        assertEquals(0, db.ascSortedListCount(key));

        assertEquals(6, db.ascSortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(1), "x".getBytes()),
                SortedListItem.of(Encoding.longToBytes(5), "y".getBytes()),
                SortedListItem.of(Encoding.longToBytes(6), "z".getBytes()),
                SortedListItem.of(Encoding.longToBytes(0), "a".getBytes()),
                SortedListItem.of(Encoding.longToBytes(4), "b".getBytes()),
                SortedListItem.of(Encoding.longToBytes(3), "c".getBytes())
        ));
        assertEquals(6, db.ascSortedListCount(key));
        final List<String> items = db.ascSortedListItems(key).stream().map(item -> String.format("%s=%s", Encoding.longFromBytes(item.score), new String(item.value))).collect(Collectors.toList());
        assertEquals(Arrays.asList("0=a", "1=x", "3=c", "4=b", "5=y", "6=z"), items);

        assertEquals("a", new String(db.ascSortedListPop(key, null).get().value));
        assertEquals("x", new String(db.ascSortedListPop(key, null).get().value));
        assertEquals("c", new String(db.ascSortedListPop(key, null).get().value));

        assertEquals(1, db.ascSortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(0), "i".getBytes()),
                SortedListItem.of(Encoding.longToBytes(1), "j".getBytes()),
                SortedListItem.of(Encoding.longToBytes(2), "k".getBytes()),
                SortedListItem.of(Encoding.longToBytes(3), "l".getBytes())
        ));
        assertEquals(4, db.ascSortedListCount(key));
        final List<String> items2 = db.ascSortedListItems(key).stream().map(item -> String.format("%s=%s", Encoding.longFromBytes(item.score), new String(item.value))).collect(Collectors.toList());
        assertEquals(Arrays.asList("3=l", "4=b", "5=y", "6=z"), items2);

        assertEquals(Optional.empty(), db.ascSortedListPop(key, Encoding.longToBytes(0)));
        assertEquals(Optional.empty(), db.ascSortedListPop(key, Encoding.longToBytes(1)));
        assertEquals(Optional.empty(), db.ascSortedListPop(key, Encoding.longToBytes(2)));
        assertEquals("l", new String(db.ascSortedListPop(key, Encoding.longToBytes(3)).get().value));
        assertEquals(Optional.empty(), db.ascSortedListPop(key, Encoding.longToBytes(3)));
        assertEquals("b", new String(db.ascSortedListPop(key, null).get().value));
        assertEquals("y", new String(db.ascSortedListPop(key, null).get().value));

        assertEquals(6, db.ascSortedListAdd(key,
                SortedListItem.of(Encoding.longToBytes(4), "A".getBytes()),
                SortedListItem.of(Encoding.longToBytes(4), "B".getBytes()),
                SortedListItem.of(Encoding.longToBytes(5), "C".getBytes()),
                SortedListItem.of(Encoding.longToBytes(5), "D".getBytes()),
                SortedListItem.of(Encoding.longToBytes(6), "E".getBytes()),
                SortedListItem.of(Encoding.longToBytes(6), "F".getBytes()),
                SortedListItem.of(Encoding.longToBytes(7), "G".getBytes()),
                SortedListItem.of(Encoding.longToBytes(7), "H".getBytes())
        ));
        assertEquals(7, db.ascSortedListCount(key));
        final List<String> items3 = db.ascSortedListItems(key).stream().map(item -> String.format("%s=%s", Encoding.longFromBytes(item.score), new String(item.value))).collect(Collectors.toList());
        assertEquals(Arrays.asList("5=C", "5=D", "6=z", "6=E", "6=F", "7=G", "7=H"), items3);

        db.ascSortedListPrune(key);
        assertEquals(7, db.ascSortedListCount(key));
        final List<String> items4 = db.ascSortedListItems(key).stream().map(item -> String.format("%s=%s", Encoding.longFromBytes(item.score), new String(item.value))).collect(Collectors.toList());
        assertEquals(Arrays.asList("5=C", "5=D", "6=z", "6=E", "6=F", "7=G", "7=H"), items4);
    }
}
