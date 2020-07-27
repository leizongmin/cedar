package com.leizm.cedar.core;

import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestUtil {
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static final List<Database> dbList = new ArrayList<>();
    private static long generateRandomKeyCounter = 0;

    public static Database createTempDatabase() {
        String path = Paths.get(
                System.getProperty("java.io.tmpdir"),
                String.format("cedar-test-%d-%d", dbList.size(), System.currentTimeMillis())
        ).toAbsolutePath().toString();
        return createTempDatabase(path);
    }

    public static Database createTempDatabase(String path) {
        try {
            System.out.printf("create database on path: %s\n", path);
            Database db = new Database(path);
            dbList.add(db);
            return db;
        } catch (RocksDBException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static void dumpDatabase(Database db) {
        System.out.println("==================== dumpDatabase ====================");
        System.out.println("path: " + db.getPath());
        RocksIterator iter = db.getDb().newIterator();
        iter.seekToFirst();
        while (iter.isValid()) {
            System.out.printf("%s (%s) = %s (%s) \n",
                    bytesToHex(iter.key()), new String(iter.key()),
                    bytesToHex(iter.value()), new String(iter.value()));
            iter.next();
        }
        System.out.println();
        iter.close();
    }

    public static List<byte[]> generateRandomKeyList(int count) {
        List<byte[]> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(generateRandomKey());
        }
        return list;
    }

    public static byte[] generateRandomKey() {
        return String.format("key-%d-%d", System.currentTimeMillis(), generateRandomKeyCounter++).getBytes();
    }
}
