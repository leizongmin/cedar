package com.leizm.cedar.core;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class LRUCache<K, V> {
    protected final int capacity;
    protected final Map<K, V> map;
    protected final List<K> list;

    public LRUCache(final int capacity) {
        this.capacity = capacity;
        this.map = new HashMap<>(capacity);
        this.list = new LinkedList<>();
    }

    public synchronized int capacity() {
        return capacity;
    }

    public synchronized int size() {
        return map.size();
    }

    public synchronized void put(K key, V value) {
        if (!map.containsKey(key)) {
            list.add(key);
        }
        map.put(key, value);
        if (list.size() > capacity) {
            K oldKey = list.get(0);
            list.remove(0);
            map.remove(oldKey);
        }
    }

    public synchronized void remove(K key) {
        map.remove(key);
        list.remove(key);
    }

    public synchronized V get(K key) {
        return map.get(key);
    }

    public synchronized void clear() {
        map.clear();
        list.clear();
    }
}
