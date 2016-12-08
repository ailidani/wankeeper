package org.wankeeper.token;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wankeeper.server.Config;

import java.util.Collection;
import java.util.Map;

public interface RWTokenManager {

    Logger LOG = LoggerFactory.getLogger(RWTokenManager.class);

    int cid = Config.instance().getClusterID();

    LeaseType type = Config.instance().getLeaseType();

    boolean contains(String token, TokenType type);

    boolean contains(Collection<String> tokens, TokenType type);

    boolean contains(Map<String, TokenType> tokens);

    void received(String token, TokenType type, int from);

    void received(Collection<String> tokens, TokenType type, int from);

    void received(Map<String, TokenType> tokens, int from);

    void remove(String token, TokenType type);

    void remove(Collection<String> tokens, TokenType type);

    void remove(Map<String, TokenType> tokens);

    void get(String token, TokenType type);

    void getAsync(String token, TokenType type);

    void get(Collection<String> tokens, TokenType type);

    void get(Map<String, TokenType> tokens);

}
