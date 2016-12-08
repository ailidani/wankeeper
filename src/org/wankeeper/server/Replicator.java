package org.wankeeper.server;

import org.apache.zookeeper.server.Request;
import org.wankeeper.message.Packet;

public interface Replicator {

    void replicate(Request request);

    void replicate(int type, Packet packet);

}
