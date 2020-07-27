package com.leizm.cedar.core;

public enum KeyType {
    Map,
    Set,
    List,
    SortedList,
    AscSortedList;

    /**
     * get KeyType from code
     *
     * @param code
     * @return KeyType
     */
    public static KeyType fromByte(byte code) {
        switch (code) {
            case 1:
                return Map;
            case 2:
                return Set;
            case 3:
                return List;
            case 4:
                return SortedList;
            case 5:
                return AscSortedList;
            default:
                throw new IllegalArgumentException(String.format("invalid code '%d'", code));
        }
    }

    public byte toByte() {
        switch (this) {
            case Map:
                return 1;
            case Set:
                return 2;
            case List:
                return 3;
            case SortedList:
                return 4;
            case AscSortedList:
                return 5;
            default:
                throw new IllegalArgumentException(String.format("invalid type '%s'", this.name()));
        }
    }
}
