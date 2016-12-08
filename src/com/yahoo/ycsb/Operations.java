package com.yahoo.ycsb;

import org.apache.zookeeper.*;

public class Operations {

    public static Status Insert(ZooKeeper zoo, String path, byte[] data, boolean print) {
        long estimatedTime;
        long startTime = System.nanoTime();
        try {
            zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            estimatedTime = System.nanoTime() - startTime;
        } catch (KeeperException.NodeExistsException e) {
            System.out.println(e.getMessage());
            return Status.ERROR;
        } catch (KeeperException e) {
            System.out.println(e.getMessage());
            return Status.ERROR;
        } catch (InterruptedException e) {
            return Status.UNEXPECTED_STATE;
        }
        if (print) {
            System.out.println("Insert " + path + " finished in " + estimatedTime + " nanoseconds");
        }
        return Status.OK;
    }

    public static Status HotInsert(ZooKeeper zoo, String path, byte[] data, boolean print) {
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
        if (print) {
            System.out.println("HotInsert " + path + " finished in " + estimatedTime + " nanoseconds");
        }
        return Status.OK;
    }

    public static Status AsyncInsert(ZooKeeper zoo, String path, byte[] data, AsyncCallback.StringCallback cb, Object ctx, boolean print) {
        zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, cb, ctx);
        if (print) {
            System.out.println("AsyncInsert " + path + " finished");
        }
        return Status.OK;
    }

    public static Status Read(ZooKeeper zoo, String path, boolean print) {
        long estimatedTime;
        long startTime = System.nanoTime();
        try {
            byte[] data = zoo.getData(path, false, null);
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
            System.out.println("Read " + path + " finished in " + estimatedTime + " nanoseconds");
        }
        return Status.OK;
    }

    public static Status SyncRead(ZooKeeper zoo, String path, boolean print) {
        long estimatedTime;
        long startTime = System.nanoTime();
        try {
            zoo.sync(path, null, null);
            byte[] data = zoo.getData(path, false, null);
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
            System.out.println("SyncRead " + path + " finished in " + estimatedTime + " nanoseconds");
        }
        return Status.OK;
    }

}
