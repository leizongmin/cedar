package com.leizm.cedar.core;

public class Box<T> {
    public T value;

    public Box(T value) {
        this.value = value;
    }

    public static <T> Box<T> of(T value) {
        return new Box<>(value);
    }
}
