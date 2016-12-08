package org.wankeeper.token;

public enum LeaseType {

    PERCENTAGE(0),
    TWO_CONSECUTIVE(2),
    THREE_CONSECUTIVE(3),
    FOUR_CONSECUTIVE(4),
    FIVE_CONSECUTIVE(5);

    private final int code;

    LeaseType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

}
