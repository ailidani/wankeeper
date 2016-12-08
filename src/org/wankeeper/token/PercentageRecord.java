package org.wankeeper.token;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PercentageRecord extends Record {

    private Queue<Integer> rhistory = new ConcurrentLinkedQueue<>();
    private Queue<Integer> whistory = new ConcurrentLinkedQueue<>();

    @Override
    boolean hit(TokenType type, int from) {
        switch (type) {
            case READ:
                rhistory.offer(from);
                break;
            case WRITE:
                whistory.offer(from);
                break;
        }
        // TODO not implemented, needs decision on queue size
        return false;
    }

}
