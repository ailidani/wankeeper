package org.wankeeper.token;

import org.wankeeper.message.PacketOld;
import org.wankeeper.server.Worker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.wankeeper.server.Master.TOKEN;

public class TokenManagerWorker extends TokenManager {

    private Set<String> requested = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private Set<String> readTokens = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Worker worker;

    public TokenManagerWorker(Worker worker) {
        this.worker = worker;
    }

    private void request(String token) {
        if (tokens.remove(token)) {
            LOG.debug("Requested a token that does not obtain");
        }
        requested.add(token);
    }

    private boolean isRequested(String token) {
        return requested.contains(token);
    }

    @Override
    public void add(String token) {
        if (isRequested(token)) {
            LOG.warn("Received token which is already requested, sending back");
            List<String> paths = new ArrayList<>();
            paths.add(token);
            //worker.send(new PacketOld(TOKEN, paths));
            requested.remove(token);
        } else {
            LOG.info("Received token: " + token);
            tokens.add(token);
        }
    }

    // FIXME send token only after commit
    @Override
    public void remove(String token) {
        this.tokens.remove(token);
        //requested.add(token);
        ArrayList<String> send = new ArrayList<>();
        send.add(token);
        //worker.send(new PacketOld(TOKEN, send));
        requested.remove(token);
    }

    @Override
    public void removeAll(Collection<String> tokens) {
        this.tokens.removeAll(tokens);
        //requested.addAll(tokens);
        List<String> send;
//        if (tokens instanceof List)
//            send = (List<String>) tokens;
//        else
            send = new ArrayList<>(tokens);
        LOG.debug("Sending token : " + tokens);
        //worker.send(new PacketOld(TOKEN, send));
    }
}
