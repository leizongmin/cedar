package com.leizm.cedar.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface IDatabase {
    Optional<byte[]> mapGet(byte[] key, byte[] field);

    long mapPut(byte[] key, MapItem... items);

    Optional<byte[]> mapRemove(byte[] key, byte[] field);

    long mapForEach(byte[] key, Consumer<MapItem> onItem);

    default List<MapItem> mapItems(byte[] key) {
        final List<MapItem> list = new ArrayList<>();
        mapForEach(key, list::add);
        return list;
    }

    long mapSize(byte[] key);

    long listLeftPush(byte[] key, byte[]... values);

    long listRightPush(byte[] key, byte[]... values);

    long listSize(byte[] key);

    Optional<byte[]> listLeftPop(byte[] key);

    Optional<byte[]> listRightPop(byte[] key);

    long listForEach(byte[] key, Consumer<ListItem> onItem);

    default List<ListItem> listItems(byte[] key) {
        final List<ListItem> list = new ArrayList<>();
        listForEach(key, list::add);
        return list;
    }

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

    long sortedListAdd(byte[] key, SortedListItem[] items);

    long sortedListSize(byte[] key);

    Optional<SortedListItem> sortedListLeftPop(byte[] key, byte[] maxScore);

    Optional<SortedListItem> sortedListRightPop(byte[] key, byte[] minScore);

    long sortedListForEach(byte[] key, Consumer<SortedListItem> onItem);

    default List<SortedListItem> sortedListItems(byte[] key) {
        final List<SortedListItem> list = new ArrayList<>();
        sortedListForEach(key, list::add);
        return list;
    }

    long forEachKeys(byte[] prefix, BiConsumer<byte[], MetaInfo> onItem);

    default long forEachKeys(BiConsumer<byte[], MetaInfo> onItem) {
        return forEachKeys(new byte[]{}, onItem);
    }
}
