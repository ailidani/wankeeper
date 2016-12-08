package org.wankeeper.token;

import org.wankeeper.message.Packet;
import org.wankeeper.server.Worker;

import java.util.*;

import static org.wankeeper.server.Master.TOKEN;
import static org.wankeeper.token.TokenType.WRITE;

public class RWTokenManagerWorker implements RWTokenManager {

    private final Map<String, TokenType> tokens = new HashMap<>();

    private final Worker worker;

    public RWTokenManagerWorker(Worker worker) {
        this.worker = worker;
    }

    @Override
    public boolean contains(String token, TokenType type) {
        return this.tokens.containsKey(token) && (this.tokens.get(token) == WRITE || this.tokens.get(token) == type);
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

    @Override
    public synchronized void received(String token, TokenType type, int from) {
        if (!this.tokens.containsKey(token)) {
            this.tokens.put(token, type);
            return;
        }
        if (type == WRITE) {
            this.tokens.put(token, type);
        }
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

    @Override
    public synchronized void remove(String token, TokenType type) {
        if (!contains(token, type)) {
            LOG.error("Do not have any token " + token);
            return;
        }
        TreeMap<String, TokenType> send = new TreeMap<>();
        switch (type) {
            case READ:
                if (!this.tokens.remove(token, type)) {
                    LOG.debug("Splitting my WRITE to reads");
                    this.tokens.put(token, type);
                }
                send.put(token, type);
                break;
            case WRITE:
                if (!this.tokens.remove(token, type)) {
                    LOG.error("Don't have the WRITE token");
                    return;
                }
                send.put(token, type);
                break;
        }
        worker.send(new Packet(TOKEN, send));
    }

    @Override
    public synchronized void remove(Collection<String> tokens, TokenType type) {
        TreeMap<String, TokenType> send = new TreeMap<>();
        for (String token : tokens) {
            if (!contains(token, type)) {
                LOG.warn("Do not have token " + token);
                continue;
            }
            switch (type) {
                case READ:
                    if (!this.tokens.remove(token, type)) {
                        LOG.debug("Splitting my WRITE to reads");
                        this.tokens.put(token, type);
                    }
                    send.put(token, type);
                    break;
                case WRITE:
                    if (!this.tokens.remove(token, type)) {
                        LOG.error("Don't have the WRITE token");
                        continue;
                    }
                    send.put(token, type);
                    break;
            }
        }
        if (!send.isEmpty())
            worker.send(new Packet(TOKEN, send));
    }

    @Override
    public synchronized void remove(Map<String, TokenType> tokens) {
        TreeMap<String, TokenType> send = new TreeMap<>();
        for (Map.Entry<String, TokenType> entry : tokens.entrySet()) {
            String token = entry.getKey();
            TokenType type = entry.getValue();
            if (!contains(token, type)) {
                LOG.warn("Do not have token " + token);
                continue;
            }
            switch (type) {
                case READ:
                    if (!this.tokens.remove(token, type)) {
                        LOG.debug("Splitting my WRITE to reads");
                        this.tokens.put(token, type);
                    }
                    send.put(token, type);
                    break;
                case WRITE:
                    if (!this.tokens.remove(token, type)) {
                        LOG.error("Don't have the WRITE token");
                        continue;
                    }
                    send.put(token, type);
                    break;
            }
        }
        if (!send.isEmpty()) {
            LOG.debug("Sending tokens to master: " + send);
            worker.send(new Packet(TOKEN, send));
        }
    }

    @Override
    public void get(String token, TokenType type) {}

    @Override
    public void getAsync(String token, TokenType type) {}

    @Override
    public void get(Collection<String> tokens, TokenType type) {}

    @Override
    public void get(Map<String, TokenType> tokens) {}
}
