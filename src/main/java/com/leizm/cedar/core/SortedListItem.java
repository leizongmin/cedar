package com.leizm.cedar.core;

public class SortedListItem {
    public final byte[] score;
    public final byte[] value;

    public SortedListItem(final byte[] score, final byte[] value) {
        this.score = score;
        this.value = value;
    }

    public static SortedListItem of(final byte[] score, final byte[] value) {
        return new SortedListItem(score, value);
    }
}
