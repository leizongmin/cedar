package com.leizm.cedar.core;

public class MapItem {
    public final byte[] field;
    public final byte[] value;

    public MapItem(final byte[] field, final byte[] value) {
        this.field = field;
        this.value = value;
    }

    public static MapItem of(final byte[] field, final byte[] value) {
        return new MapItem(field, value);
    }
}
