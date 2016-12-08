package org.wankeeper.token;

import org.wankeeper.message.PacketOld;
import org.wankeeper.server.Config;
import org.wankeeper.server.Master;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import static org.wankeeper.server.Master.REQUEST;

public class TokenManagerMaster extends TokenManager {

    private LeaseType type;
    private final Map<String, Integer> tokenMap = new HashMap<>();
    private final Map<String, Integer> requests = new ConcurrentHashMap<>();
    private final Map<String, Record> history = new ConcurrentHashMap<>();
    private final Map<String, Queue<Integer>> history2 = new ConcurrentHashMap<>();
    private final Master master;

    private static class Record {
        int cid;
        AtomicInteger count = new AtomicInteger(1);
        public Record(int cid) { this.cid = cid; }
        public void add() { count.incrementAndGet(); }
    }

    public TokenManagerMaster(Master master) {
        this(master, Config.instance().getLeaseType());
    }

    public TokenManagerMaster(Master master, LeaseType type) {
        this.master = master;
        this.type = type;
    }

    public boolean saveToHistory(String path, int cid) {
        if (type == LeaseType.PERCENTAGE) {
            if (!history2.containsKey(path)) {
                history2.put(path, new ConcurrentLinkedQueue<>());
            }
            history2.get(path).offer(cid);
            if (history2.get(path).size() >= 10) history2.get(path).poll();
            // TODO not implemented
            System.exit(-1);
        }
        if (!history.containsKey(path)) {
            history.put(path, new Record(cid));
            tokens.add(path);
            tokenMap.put(path, Config.instance().getClusterID());
        } else {
            if (history.get(path).cid == cid)
                history.get(path).add();
            else
                history.put(path, new Record(cid));
        }
        if (history.get(path).cid == cid && history.get(path).count.get() >= type.getCode()) {
            LOG.info("path = " + path + " has " + type.name() + " history");
            tokenMap.put(path, cid);
            if (cid == Config.instance().getClusterID()) {
                tokens.add(path);
            }
            return true;
        }
        return false;
    }

    @Override
    public synchronized void add(String token) {
        tokens.add(token);
        LOG.info("Received token: " + token);
        if (requests.containsKey(token)) {
            synchronized (requests.get(token)) {
                requests.get(token).notifyAll();
                requests.remove(token);
            }
        }
    }

    public void get(String token) {
        if (contains(token)) return;
        synchronized (tokenMap) {
            if (contains(token)) return;
            if (!tokenMap.containsKey(token)) {
                tokenMap.put(token, Config.instance().getClusterID());
                tokens.add(token);
                return;
            }
            int cid = tokenMap.get(token);
            List<String> paths = new ArrayList<>();
            paths.add(token);
            requests.put(token, cid);
            //master.send(cid, new PacketOld(REQUEST, paths));
        }
        while (!contains(token)) {
            synchronized (requests.get(token)) {
                try { requests.get(token).wait(); }
                catch (InterruptedException e) { }
            }
        }
    }

    public void getAsync(String token) {
        if (contains(token)) return;
        synchronized (tokenMap) {
            if (contains(token)) return;
            if (!tokenMap.containsKey(token)) {
                tokenMap.put(token, Config.instance().getClusterID());
                tokens.add(token);
                return;
            }
            int cid = tokenMap.get(token);
            List<String> paths = new ArrayList<>();
            paths.add(token);
            //master.send(cid, new PacketOld(REQUEST, paths));
        }
    }

    public void getAll(Collection<String> tokens) {
        if (containsAll(tokens) || tokens.isEmpty()) return;
        HashMap<Integer, ArrayList<String>> send = new HashMap<>();
        synchronized (tokenMap) {
            if (containsAll(tokens)) return;
            for (String token : tokens) {
                if (contains(token)) continue;
                if (!tokenMap.containsKey(token)) {
                    tokenMap.put(token, Config.instance().getClusterID());
                    this.tokens.add(token);
                    continue;
                }
                int cid = tokenMap.get(token);
                if (!send.containsKey(cid)) {
                    ArrayList<String> p = new ArrayList<>();
                    p.add(token);
                    send.put(cid, p);
                } else {
                    send.get(cid).add(token);
                }
                requests.put(token, cid);
            }

        }
        Iterator<Map.Entry<Integer, ArrayList<String>>> it = send.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ArrayList<String>> pairs = it.next();
            //master.send(pairs.getKey(), new PacketOld(REQUEST, pairs.getValue()));
            it.remove();
        }
//        while (!containsAll(tokens)) {
//            for (String token : tokens) {
//                if (!contains(token)) {
//                    synchronized (requests.get(token)) {
//                        try {
//                            requests.get(token).wait();
//                            break;
//                        } catch (InterruptedException e) {
//                        }
//                    }
//                }
//            }
//        }
        //while (!containsAll(tokens)) {
            for (String token : tokens) {
                if (!contains(token) && requests.containsKey(token)) {
                    synchronized (requests.get(token)) {
                        try {
                            requests.get(token).wait();
                        }
                        catch (InterruptedException e) {}
                    }
                } else if (!contains(token) && !requests.containsKey(token)) {
                    LOG.info("token missing");
                }
            }
        //}
    }
}
