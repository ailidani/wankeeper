package com.yahoo.ycsb;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestOneAsync {

    private static final String CONNECT = "wankeeper.connect";
    private static final String CONNECT_DEFAULT = "localhost:2181";
    private static final String PATH = "/test";
    private static final byte[] DATA = "testtest".getBytes();
    private ZooKeeper zoo;
    private AtomicInteger completedRequests = new AtomicInteger(0);
    private long start;

    private void connect() {
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
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Keeper connected.");
    }

    private void create() {
        long startTime = System.nanoTime();
        zoo.create(PATH, DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                long start = (long) ctx;
                long newTime = System.nanoTime() - start;
                if (rc == 0) {
                    completedRequests.incrementAndGet();
                    System.out.println("Update " + path + " finished in " + newTime + " nanoseconds");
                }
            }
        }, startTime);

    }

    private void set() {
        long startTime = System.nanoTime();
        zoo.setData(PATH, DATA, -1, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                long start = (long) ctx;
                long newTime = System.nanoTime() - start;
                if (rc == 0) {
                    completedRequests.incrementAndGet();
                    System.out.println("Update " + path + " finished in " + newTime + " nanoseconds");
                }
            }
        }, startTime);
    }

    public static void main(String[] args) throws IOException {

        TestOneAsync test = new TestOneAsync();
        test.connect();

        System.in.read();

        test.create();

        while (true) {
            System.in.read();
            test.set();
        }


    }
}
