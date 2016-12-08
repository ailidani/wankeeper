package com.yahoo.ycsb;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.*;

public class ZooKeeperDB extends DB implements Watcher {

    private static final String CONNECT = "wankeeper.connect";
    private static final String CONNECT_DEFAULT = "localhost:2181";
    private ZooKeeper zoo;

    private boolean hotstart = false;
    private boolean syncread = false;
    private boolean print = false;

    // For TXN in scan operation
    private Random random = new Random();
    private int lower = 0;
    private int upper = 0;

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

    AsyncCallback.StringCallback insertcb = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            if (rc == 0) {
                System.out.println("Insert " + path + " finished.");
            }
        }
    };

    @Override
    public void init() throws DBException {
        super.init();
        Properties properties = getProperties();
        hotstart = Boolean.valueOf(properties.getProperty("hotstart", System.getProperty("hotstart", "false")));
        syncread = Boolean.valueOf(properties.getProperty("syncread", System.getProperty("syncread", "false")));
        print = Boolean.valueOf(properties.getProperty("print", System.getProperty("print", "false")));
        lower = Integer.parseInt(properties.getProperty("hotspotlower"));
        upper = Integer.parseInt(properties.getProperty("hotspotupper"));
        String connect;
        if (properties.containsKey(CONNECT)) {
            connect = properties.getProperty(CONNECT, CONNECT_DEFAULT);
        } else {
            connect = System.getProperty(CONNECT, CONNECT_DEFAULT);
        }
        try {
            zoo = new ZooKeeper(connect, 300000, this);
            //zoo.create("/usertable", null, null, CreateMode.PERSISTENT);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DBException();
        }
        System.out.println("Keeper connected.");
    }

    @Override
    public void cleanup() throws DBException {
        super.cleanup();
        try {
            zoo.close();
        } catch (InterruptedException e) {
        }
    }


    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        String path = createPath(key);

        if (syncread) {
            return Operations.SyncRead(zoo, path, print);
        } else {
            return Operations.Read(zoo, path, print);
        }
    }


    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> results) {
        if (!getProperties().getProperty("insertorder").equals("ordered"))
            return Status.NOT_IMPLEMENTED;

        String path = createPath(startkey);
        int base = Integer.valueOf(path.replaceAll("\\D+", ""));
        long estimatedTime;
        long startTime = System.nanoTime();
        try {
            Transaction txn = zoo.transaction();
            for (int i=0; i<recordcount; i++) {
                int r = random.nextInt(upper - lower) + lower;
                //int r = (base + i) % 1000;
                System.out.print(r + " ");
                txn.setData("/user" + String.valueOf(r), new byte[100], -1);
            }
            txn.commit();
            estimatedTime = System.nanoTime() - startTime;
            System.out.println();
        } catch (KeeperException e) {
            System.out.println(e.getMessage());
            return Status.ERROR;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.UNEXPECTED_STATE;
        }
        System.out.println("Update " + recordcount + " finished in " + estimatedTime + " nanoseconds");
        return Status.OK;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        String path = createPath(key);
        byte[] data = values.values().iterator().next().toArray();
        long estimatedTime;
        long startTime = System.nanoTime();
        try {
            zoo.setData(path, data, -1);
            estimatedTime = System.nanoTime() - startTime;
        } catch (KeeperException.NoNodeException e) {
            System.out.println(e.getMessage());
            return Status.NOT_FOUND;
        } catch (KeeperException e) {
            System.out.println(e.getMessage());
            return Status.ERROR;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.UNEXPECTED_STATE;
        }
        if (print) {
            System.out.println("Update " + path + " finished in " + estimatedTime + " nanoseconds");
        }
        return Status.OK;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        String path = createPath(key);
        byte[] data = values.values().iterator().next().toArray();

        if (hotstart) {
            return Operations.HotInsert(zoo, path, data, print);
        } else {
            return Operations.Insert(zoo, path, data, print);
        }
    }

    @Override
    public Status delete(String table, String key) {
        String path = createPath(key);
        long estimatedTime;
        long startTime;
        try {
//            List<String> children = zoo.getChildren(path, false);
//            for (String child : children) {
//                zoo.delete(child, -1);
//            }
            startTime = System.nanoTime();
            zoo.delete(path, -1);
            estimatedTime = System.nanoTime() - startTime;
        } catch (KeeperException e) {
            System.out.println(e.getMessage());
            return Status.ERROR;
        } catch (InterruptedException e) {
            return Status.UNEXPECTED_STATE;
        }
        //System.out.println("Delete " + path + " finished in " + estimatedTime + " nanoseconds");
        return Status.OK;
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO
    }
}
