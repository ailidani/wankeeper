package org.wankeeper.token;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wankeeper.server.Config;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TokenManager {

    protected static final Logger LOG = LoggerFactory.getLogger(TokenManager.class);

    protected final Set<String> tokens = Collections.newSetFromMap(new ConcurrentHashMap<>());
    protected int cid = Config.instance().getClusterID();
    protected static long startTime = System.nanoTime();
    public long namoTime() { return System.nanoTime() - startTime; }

    public boolean contains(String token) {
        return this.tokens.contains(token);
    }

    public boolean containsAll(Collection<String> tokens) { return this.tokens.containsAll(tokens); }

    public abstract void add(String token);

    public void addAll(Collection<String> tokens) {
        for (String token : tokens) {
            add(token);
        }
    }

    public void remove(String token) {
        this.tokens.remove(token);
    }

    public void removeAll(Collection<String> tokens) {
        this.tokens.removeAll(tokens);
    }

}
