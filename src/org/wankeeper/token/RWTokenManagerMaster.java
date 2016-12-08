package org.wankeeper.token;

import org.wankeeper.message.Packet;
import org.wankeeper.server.Config;
import org.wankeeper.server.Master;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.wankeeper.server.Master.REQUEST;
import static org.wankeeper.token.TokenType.READ;

public class RWTokenManagerMaster implements RWTokenManager {

    private final Map<String, Set<Integer>> tokens = new ConcurrentHashMap<>();

    private final Map<String, Record> history = new HashMap<>();

    private final Set<String> requests = new HashSet<>();

    private final Master master;

    public RWTokenManagerMaster(Master master) {
        this.master = master;
    }

    @Override
    public boolean contains(String token, TokenType type) {
        if (!this.tokens.containsKey(token)) {
            this.tokens.put(token, new HashSet<>());
            this.tokens.get(token).add(cid);
        }
        switch (type) {
            case READ:
                return this.tokens.get(token).contains(cid);
            case WRITE:
                return this.tokens.get(token).contains(cid) && this.tokens.get(token).size() == 1;
            default:
                LOG.error("Unknown token type");
                return false;
        }
    }

    @Override
    public synchronized boolean contains(Collection<String> tokens, TokenType type) {
        for (String token : tokens) {
            if (!contains(token, type))
                return false;
        }
        return true;
    }

    @Override
    public synchronized boolean contains(Map<String, TokenType> tokens) {
        for (Map.Entry<String, TokenType> entry : tokens.entrySet()) {
            if (!contains(entry.getKey(), entry.getValue()))
                return false;
        }
        return true;
    }

    public synchronized void add(String token, TokenType type) {
        if (!this.tokens.containsKey(token)) {
            this.tokens.put(token, new HashSet<>());
        }
        switch (type) {
            case READ:
                this.tokens.get(token).add(cid);
                break;
            case WRITE:
                this.tokens.get(token).clear();
                this.tokens.get(token).add(cid);
                break;
            default:
                LOG.error("Unknown token type");
                break;
        }
    }

    public synchronized void received(String token, TokenType type, int from) {
        // TODO received tokens should already exists in token map
        LOG.debug("Received {} token: {} from {}.", type, token, from);
        switch (type) {
            case READ:
                if (this.tokens.get(token).size() == 1) this.tokens.get(token).add(cid);
                else this.tokens.get(token).remove(from);
                break;
            case WRITE:
                this.tokens.get(token).clear();
                this.tokens.get(token).add(cid);
                break;
            default:
                LOG.error("Unknown token type");
                break;
        }
        if (this.requests.contains(token))
            notifyAll();
    }

    @Override
    public synchronized void received(Collection<String> tokens, TokenType type, int from) {
        for (String token : tokens) {
            received(token, type, from);
        }
    }

    @Override
    public synchronized void received(Map<String, TokenType> tokens, int from) {
        for (Map.Entry<String, TokenType> entry : tokens.entrySet()) {
            received(entry.getKey(), entry.getValue(), from);
        }
    }

    public synchronized void put(Map<String, TokenType> tokens, int cid) {
        for (Map.Entry<String, TokenType> entry : tokens.entrySet()) {
            String token = entry.getKey();
            TokenType type = entry.getValue();
            if (!this.tokens.containsKey(token)) {
                this.tokens.put(token, new HashSet<>());
                this.tokens.get(token).add(Config.instance().getClusterID());
            }
            switch (type) {
                case READ:
                    this.tokens.get(token).add(cid);
                    LOG.debug("Add READ token to {}.", cid);
                    break;
                case WRITE:
                    this.tokens.get(token).clear();
                    this.tokens.get(token).add(cid);
                    LOG.debug("Add WRITE token to {}.", cid);
                    break;
            }
        }
    }

    @Override
    public synchronized void remove(String token, TokenType type) {
        if (!contains(token, type)) return;
        switch (type) {
            case READ:
                this.tokens.get(token).remove(cid);
                LOG.debug("Remove my READ token");
                break;
            case WRITE:
                this.tokens.get(token).clear();
                LOG.debug("Remove my WRITE token");
                break;
            default:
                LOG.error("Unknown token type");
                break;
        }
    }

    @Override
    public synchronized void remove(Collection<String> tokens, TokenType type) {
        for (String token : tokens) {
            remove(token, type);
        }
    }

    @Override
    public synchronized void remove(Map<String, TokenType> tokens) {
        for (Map.Entry<String, TokenType> entry : tokens.entrySet()) {
            remove(entry.getKey(), entry.getValue());
        }
    }

    public synchronized boolean hit(String token, TokenType type, int from) {
        if (!history.containsKey(token)) {
            Record record;
            switch (Config.instance().getLeaseType()) {
                case PERCENTAGE:
                    record = new PercentageRecord();
                    break;
                case TWO_CONSECUTIVE:
                case THREE_CONSECUTIVE:
                case FOUR_CONSECUTIVE:
                case FIVE_CONSECUTIVE:
                    record = new FlagRecord();
                    break;
                default:
                    LOG.error("Unknown Lease Type");
                    record = new ConsecutiveRecord();
                    break;
            }
            history.put(token, record);
        }
//        if (history.get(token).hit(type, from)) {
//            if (type == READ) this.tokens.get(token).add(from);
//            else {
//                this.tokens.get(token).clear();
//                this.tokens.get(token).add(from);
//            }
//            return true;
//        }
//        return false;
        return history.get(token).hit(type, from);
    }

    @Override
    public synchronized void getAsync(String token, TokenType type) {
        if (contains(token, type)) return;
        for (int i : this.tokens.get(token)) {
            if (i == cid) continue;
            TreeMap<String, TokenType> send = new TreeMap<>();
            if (this.tokens.get(token).size() == 1)
                send.put(token, type);
            else send.put(token, READ);
            master.send(i, new Packet(REQUEST, send));
        }
    }

    @Override
    public synchronized void get(String token, TokenType type) {
        if (contains(token, type)) return;
        // type we need might not be the same as type in REQUEST
        this.requests.add(token);
        for (int i : this.tokens.get(token)) {
            if (i == cid) continue;
            TreeMap<String, TokenType> send = new TreeMap<>();
            if (this.tokens.get(token).size() == 1)
                send.put(token, type);
            else send.put(token, READ);
            master.send(i, new Packet(REQUEST, send));
            LOG.debug("Getting {} token: {} from {}.", type, token, i);
        }
        while (!contains(token, type)) {
            try { wait(); }
            catch (InterruptedException e) { }
        }
        this.requests.remove(token);
    }

    @Override
    public synchronized void get(Collection<String> tokens, TokenType type) {
        if (contains(tokens, type)) return;
        Map<Integer, TreeMap<String, TokenType>> sends = new HashMap<>();
        for (String token : tokens) {
            if (contains(token, type)) continue;
            for (int i : this.tokens.get(token)) {
                if (i == cid) continue;
                if (!sends.containsKey(i)) sends.put(i, new TreeMap<>());
                if (this.tokens.get(token).size() == 1)
                    sends.get(i).put(token, type);
                else sends.get(i).put(token, READ);
            }
        }
        this.requests.addAll(tokens);
        for (int i : sends.keySet()) {
            master.send(i, new Packet(REQUEST, sends.get(i)));
        }
        while (!contains(tokens, type)) {
            try { wait(); }
            catch (InterruptedException e) { }
        }
        this.requests.removeAll(tokens);
    }

    @Override
    public synchronized void get(Map<String, TokenType> tokens) {
        if (contains(tokens)) return;
        Map<Integer, TreeMap<String, TokenType>> sends = new HashMap<>();
        for (Map.Entry<String, TokenType> entry : tokens.entrySet()) {
            if (contains(entry.getKey(), entry.getValue())) continue;
            for (int i : this.tokens.get(entry.getKey())) {
                if (i == cid) continue;
                if (!sends.containsKey(i)) sends.put(i, new TreeMap<>());
                if (this.tokens.get(entry.getKey()).size() == 1)
                    sends.get(i).put(entry.getKey(), entry.getValue());
                else sends.get(i).put(entry.getKey(), READ);
            }
        }
        this.requests.addAll(tokens.keySet());
        for (int i : sends.keySet()) {
            master.send(i, new Packet(REQUEST, sends.get(i)));
        }
        while (!contains(tokens)) {
            try { wait(); }
            catch (InterruptedException e) { }
        }
        this.requests.removeAll(tokens.keySet());
    }
}
