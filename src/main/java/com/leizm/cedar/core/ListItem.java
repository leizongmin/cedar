package com.leizm.cedar.core;

public class ListItem {
    public final long index;
    public final byte[] value;

    public ListItem(final long index, final byte[] value) {
        this.index = index;
        this.value = value;
    }

    public static ListItem of(final long index, final byte[] value) {
        return new ListItem(index, value);
    }
}
