package com.leizm.cedar.core;

import org.iq80.leveldb.DBIterator;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestUtil {
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static List<Database> dbList = new ArrayList<>();
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

    public static void dumpDatabase(Database db) {
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
