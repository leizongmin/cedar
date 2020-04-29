package com.leizm.cedar.core;

public class DatabaseBenchmark {
    private static final int COUNT = 10_0000;

    public static void main(String[] argv) {
        testMap();
        testSet();
        testList();
        testSortedList();
    }

    private static void runTestCase(final String title, final int count, final TestCaseFunction fn) {
        long start = System.currentTimeMillis();
        fn.apply(count);
        long spent = System.currentTimeMillis() - start;
        System.out.printf("%20s %d times,\tspent %6d ms,\t%8d op/s\n", title, count, spent, count / spent * 1000);
    }

    public static void testMap() {
        final Database db = TestUtil.createTempDatabase();
        final byte[] key = TestUtil.generateRandomKey();
        runTestCase("db.mapPut", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.mapPut(key, MapItem.of(v, v));
            }
        });
        runTestCase("db.mapGet", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.mapGet(key, v);
            }
        });
        runTestCase("db.mapCount", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                db.mapCount(key);
            }
        });
        runTestCase("db.mapRemove", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.mapRemove(key, v);
            }
        });
        System.out.println();
    }

    public static void testSet() {
        final Database db = TestUtil.createTempDatabase();
        final byte[] key = TestUtil.generateRandomKey();
        runTestCase("db.setAdd", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.setAdd(key, v);
            }
        });
        runTestCase("db.setIsMember", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.setIsMember(key, v);
            }
        });
        runTestCase("db.setCount", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                db.setCount(key);
            }
        });
        runTestCase("db.setRemove", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.setRemove(key, v);
            }
        });
        System.out.println();
    }

    public static void testList() {
        final Database db = TestUtil.createTempDatabase();
        final byte[] key = TestUtil.generateRandomKey();
        runTestCase("db.listLeftPush", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.listLeftPush(key, v);
            }
        });
        runTestCase("db.listRightPush", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.listRightPush(key, v);
            }
        });
        runTestCase("db.listCount", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                db.listCount(key);
            }
        });
        runTestCase("db.listLeftPop", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                db.listLeftPop(key);
            }
        });
        runTestCase("db.listRightPop", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                db.listRightPop(key);
            }
        });
        System.out.println();
    }

    public static void testSortedList() {
        final Database db = TestUtil.createTempDatabase();
        final byte[] key = TestUtil.generateRandomKey();
        runTestCase("db.sortedListAdd", COUNT * 2, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.sortedListAdd(key, SortedListItem.of(v, v));
            }
        });
        runTestCase("db.sortedListCount", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                db.sortedListCount(key);
            }
        });
        runTestCase("db.sortedListLeftPop", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.sortedListLeftPop(key, v);
            }
        });
        runTestCase("db.sortedListRightPop", COUNT, count -> {
            for (int i = 0; i < count; i++) {
                final byte[] v = Integer.toString(i).getBytes();
                db.sortedListRightPop(key, v);
            }
        });
        System.out.println();
    }

    @FunctionalInterface
    private interface TestCaseFunction {
        void apply(int count);
    }
}
