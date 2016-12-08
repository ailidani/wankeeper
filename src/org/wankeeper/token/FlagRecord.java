package org.wankeeper.token;

import java.util.concurrent.atomic.AtomicInteger;

public class FlagRecord extends Record {

    private boolean readFlag = false;
    private AtomicInteger rcount = new AtomicInteger(0);
    private int cid = -1;
    private AtomicInteger wcount = new AtomicInteger(0);

    @Override
    boolean hit(TokenType type, int from) {
        switch (type) {
            case READ:
                if (rcount.incrementAndGet() >= this.type.getCode()) {
                    readFlag = true;
                }
                return readFlag;
            case WRITE:
                if (from == cid) wcount.incrementAndGet();
                else {
                    cid = from;
                    wcount.set(1);
                }
                if (wcount.get() >= this.type.getCode()) {
                    readFlag = false;
                    return true;
                }
                return false;
            default:
                LOG.error("Unknown Token Type");
                return false;
        }
    }
}
