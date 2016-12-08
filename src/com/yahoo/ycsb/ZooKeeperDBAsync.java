package com.yahoo.ycsb;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ZooKeeperDBAsync extends DB {

    private static final String CONNECT = "wankeeper.connect";
    private static final String CONNECT_DEFAULT = "localhost:2181";
    private ZooKeeper zoo;
    Properties properties;
    private AtomicInteger sent = new AtomicInteger(0);
    private List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private AtomicInteger completedRequests = new AtomicInteger(0);
    private long start;

    private static String createPath(String key) {
        return "/" + key.replaceAll("[^A-Za-z0-9]", "");
    }

    private static String createPath(String table, String key) {
        return "/" + table.replaceAll("[^A-Za-z0-9]", "") + "/" + key.replaceAll("[^A-Za-z0-9]", "");
    }

    private static String createPath(String table, String key, String field) {
        return "/" + table.replaceAll("[^A-Za-z0-9]", "")
                + "/" + key.replaceAll("[^A-Za-z0-9]", "")
                + "/" + field.replaceAll("[^A-Za-z0-9]", "");
    }

    static class Context {
        long localStartTime;
        int id;

        Context(int id, long time) {
            this.id = id;
            this.localStartTime = time;
        }
    }

    AsyncCallback.DataCallback readcb = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            Context context = (Context) ctx;
            long newTime = System.nanoTime() - context.localStartTime;
            if (rc == 0) {
                latencies.add(newTime);
                completedRequests.incrementAndGet();
                System.out.println("Update " + path + " finished in " + newTime + " nanoseconds");
            }
        }
    };

    AsyncCallback.StatCallback updatecb = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            Context context = (Context) ctx;
            long newTime = System.nanoTime() - context.localStartTime;
            if (rc == 0) {
                latencies.add(newTime);
                completedRequests.incrementAndGet();
                System.out.println("Update " + path + " finished in " + newTime + " nanoseconds");
            }
        }
    };

    AsyncCallback.StringCallback insertcb = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            Context context = (Context) ctx;
            long newTime = System.nanoTime() - context.localStartTime;
            if (rc == 0) {
                latencies.add(newTime);
                completedRequests.incrementAndGet();
                System.out.println("Update " + path + " finished in " + newTime + " nanoseconds");
            }
        }
    };

    AsyncCallback.VoidCallback deletecb = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            Context context = (Context) ctx;
            long newTime = System.nanoTime() - context.localStartTime;
            if (rc == 0) {
                latencies.add(newTime);
                completedRequests.incrementAndGet();
                System.out.println("Update " + path + " finished in " + newTime + " nanoseconds");
            }
        }
    };

    private static double percentile(long[] latency, int percentile) {
        int size = latency.length;
        int sampleSize = (size * percentile) / 100;
        long total = 0;
        int count = 0;
        for(int i = 0; i < sampleSize; i++) {
            total += latency[i];
            count++;
        }
        return ((double)total/(double)count)/1000000.0;
    }

    @Override
    public void init() throws DBException {
        super.init();
        properties = getProperties();
        //String connect = properties.getProperty(CONNECT, CONNECT_DEFAULT);
        String connect = System.getProperty(CONNECT, CONNECT_DEFAULT);
        final CountDownLatch connectLatch = new CountDownLatch(1);
        try {
            zoo = new ZooKeeper(connect, 300000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        connectLatch.countDown();
                    }
                }
            });
            if (!connectLatch.await(10, TimeUnit.SECONDS)) {
                System.err.println("Couldn't connect to zookeeper at " + connect);
                zoo.close();
                System.exit(-1);
            }
            //zoo.create("/usertable", null, null, CreateMode.PERSISTENT);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            throw new DBException();
        }
        System.out.println("Keeper connected.");
        this.start = System.currentTimeMillis();
    }

    @Override
    public void cleanup() throws DBException {
        //super.cleanup();

        //while(completedRequests.get() < Integer.valueOf(System.getProperty("ops"))) {
        while (completedRequests.get() < sent.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}
        }

        long duration = System.currentTimeMillis() - start;
        System.out.println("Completed Requests = " + completedRequests.get());
        long[] latency = new long[latencies.size()];
        long sum = 0;
        for (int i=0; i<latencies.size(); i++) {
            latency[i] = latencies.get(i);
            sum += latency[i];
        }
        System.out.println("Average latency: " + ((double)sum / (double)latency.length));

        Arrays.sort(latency);

        long throughput = (long)((double)(completedRequests.get()*1000.0)/(double) duration);
        System.out.println(completedRequests.get() + " completions in " + duration + " milliseconds: " + throughput + " ops/sec");

        try {
            // dump the latencies for later debugging (it will be sorted by entryid)
            OutputStream fos = new BufferedOutputStream(new FileOutputStream("latencyDump.dat"));
            for (Long l : latencies) {
                fos.write((Long.toString(l) + "\t" + (l / 1000000) + "ms\n").getBytes(UTF_8));
            }
            fos.flush();
            fos.close();
        } catch (IOException e) {
        }

        // now get the latencies
        System.out.println("99th percentile latency: " + percentile(latency, 99));
        System.out.println("95th percentile latency: " + percentile(latency, 95));
        System.out.println();
        try {
            latencies.clear();
            zoo.close();
        } catch (InterruptedException e) {
        }
    }


    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        String path = createPath(key);
        long startTime = System.nanoTime();
        zoo.getData(path, false, readcb, new Context(sent.incrementAndGet(), startTime));
        return Status.OK;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> results) {
        return Status.NOT_IMPLEMENTED;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        String path = createPath(key);
        byte[] data = values.values().iterator().next().toArray();
        long startTime = System.nanoTime();
        zoo.setData(path, data, -1, updatecb, new Context(sent.incrementAndGet(), startTime));
        return Status.OK;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        String path = createPath(key);
        byte[] data = values.values().iterator().next().toArray();
        long startTime = System.nanoTime();
        //zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, insertcb, new Context(sent.incrementAndGet(), startTime));
        return Status.OK;
    }

    @Override
    public Status delete(String table, String key) {
        String path = createPath(key);
        long startTime = System.nanoTime();
        zoo.delete(path, -1, deletecb, new Context(sent.incrementAndGet(), startTime));
        return Status.OK;
    }

}
