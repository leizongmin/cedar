package com.leizm.cedar.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface IDatabase {
    Optional<byte[]> mapGet(byte[] key, byte[] field);

    long mapPut(byte[] key, byte[]... pairs);

    Optional<byte[]> mapRemove(byte[] key, byte[] field);

    long mapForEach(byte[] key, BiConsumer<byte[], byte[]> onItem);

    long mapSize(byte[] key);

    long listLeftPush(byte[] key, byte[]... values);

    long listRightPush(byte[] key, byte[]... values);

    long listSize(byte[] key);

    Optional<byte[]> listLeftPop(byte[] key);

    Optional<byte[]> listRightPop(byte[] key);

    long listForEach(byte[] key, Consumer<byte[]> onItem);

    long setAdd(byte[] key, byte[]... values);

    boolean setIsMember(byte[] key, byte[]... values);

    long setRemove(byte[] key, byte[]... values);

    long setSize(byte[] key);

    long setForEach(byte[] key, Consumer<byte[]> onItem);

    default List<byte[]> setMembers(byte[] key) {
        final List<byte[]> list = new ArrayList<>();
        setForEach(key, list::add);
        return list;
    }

    long sortedListAdd(byte[] key, byte[]... pairs);

    long sortedListSize(byte[] key);

    Optional<byte[]> sortedListLeftPop(byte[] key, byte[] maxScore);

    Optional<byte[]> sortedListRightPop(byte[] key, byte[] minScore);

    long sortedListForEach(byte[] key, Consumer<byte[]> onItem);

    long forEachKeys(byte[] prefix, BiConsumer<byte[], MetaInfo> onItem);

    default long forEachKeys(BiConsumer<byte[], MetaInfo> onItem) {
        return forEachKeys(new byte[]{}, onItem);
    }
}
