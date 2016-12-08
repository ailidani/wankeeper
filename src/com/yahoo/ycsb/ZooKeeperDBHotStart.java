package com.yahoo.ycsb;

import org.apache.zookeeper.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

public class ZooKeeperDBHotStart extends DB implements Watcher {

    private static final String CONNECT = "wankeeper.connect";
    private static final String CONNECT_DEFAULT = "localhost:2181";
    private ZooKeeper zoo;

    private static String createPath(String key) {
        return "/" + key.replaceAll("[^A-Za-z0-9]", "");
    }

    @Override
    public void init() throws DBException {
        super.init();
        //Properties properties = getProperties();
        //String connect = properties.getProperty(CONNECT, CONNECT_DEFAULT);
        String connect = System.getProperty(CONNECT, CONNECT_DEFAULT);
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
        long estimatedTime;
        long startTime = System.nanoTime();
        try {
            byte[] data = zoo.getData(path, false, null);
            estimatedTime = System.nanoTime() - startTime;
            result.put(key, new ByteArrayByteIterator(data));
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
        System.out.println("Read finished in " + estimatedTime + " nanoseconds");
        return Status.OK;
    }

    @Override
    public Status scan(String table, String key, int i, Set<String> fields, Vector<HashMap<String, ByteIterator>> results) {
        return Status.NOT_IMPLEMENTED;
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
        System.out.println("Update finished in " + estimatedTime + " nanoseconds");
        return Status.OK;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        String path = createPath(key);
        byte[] data = values.values().iterator().next().toArray();
        long estimatedTime;
        long startTime = System.nanoTime();
        try {
            zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            estimatedTime = System.nanoTime() - startTime;
            zoo.setData(path, data, -1);
        } catch (KeeperException.NodeExistsException e) {
            System.out.println(e.getMessage());
            return Status.ERROR;
        } catch (KeeperException e) {
            System.out.println(e.getMessage());
            return Status.ERROR;
        } catch (InterruptedException e) {
            return Status.UNEXPECTED_STATE;
        }
        System.out.println("Insert " + path + " finished in " + estimatedTime + " nanoseconds");
        return Status.OK;
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
        System.out.println("Delete finished in " + estimatedTime + " nanoseconds");
        return Status.OK;
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO
    }

}
