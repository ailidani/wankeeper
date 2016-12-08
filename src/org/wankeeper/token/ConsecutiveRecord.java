package org.wankeeper.token;

import java.util.concurrent.atomic.AtomicInteger;


public class ConsecutiveRecord extends Record {

    // FIXME one cid for both read and write???
    private int rcid;
    private int wcid;
    private AtomicInteger rcount = new AtomicInteger(0);
    private AtomicInteger wcount = new AtomicInteger(0);

    @Override
    public boolean hit(TokenType type, int from) {
        switch (type) {
            case READ:
                if (from == rcid) rcount.incrementAndGet();
                else {
                    rcid = from;
                    rcount.set(1);
                }
                return rcount.get() >= this.type.getCode();
            case WRITE:
                if (from == wcid) wcount.incrementAndGet();
                else {
                    wcid = from;
                    wcount.set(1);
                }
                return wcount.get() >= this.type.getCode();
            default:
                LOG.error("Unknown Token Type");
                return false;
        }
    }

}
