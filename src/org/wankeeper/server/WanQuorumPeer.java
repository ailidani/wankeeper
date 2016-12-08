package org.wankeeper.server;

import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.QuorumPeer;

import java.io.IOException;

public class WanQuorumPeer extends QuorumPeer {

    @Override
    protected Leader makeLeader(FileTxnSnapLog logFactory) throws IOException {
        return new Leader(this, new LeaderWanKeeperServer(logFactory, this, this.zkDb));
    }

}
