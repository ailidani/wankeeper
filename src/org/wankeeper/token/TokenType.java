package org.wankeeper.token;

public enum TokenType {

    // read token
    READ(1),
    // write token
    WRITE(2);

    private final int code;

    TokenType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static TokenType fromInt(int code) {
        switch (code) {
            case 1:
                return READ;
            case 2:
                return WRITE;
            default:
                return WRITE;
        }
    }
}
