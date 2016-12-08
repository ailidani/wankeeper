package org.wankeeper.server;

import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeer;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;

public class ManagedQuorumPeer {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedQuorumPeer.class);

    private final QuorumPeer quorumPeer;

    public ManagedQuorumPeer(final QuorumPeerConfig config,
                             final ServerCnxnFactory cnxnFactory,
                             final ServerCnxnFactory secureCnxnFactory,
                             final FileTxnSnapLog fileTxnSnapLog) {
        try {
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        if (Config.instance().getClusterID() == 0) {
            quorumPeer = new QuorumPeer();
        } else {
            quorumPeer = new WanQuorumPeer();
        }
        quorumPeer.setTxnFactory(fileTxnSnapLog);
        quorumPeer.enableLocalSessions(config.areLocalSessionsEnabled());
        quorumPeer.enableLocalSessionsUpgrading(
                config.isLocalSessionsUpgradingEnabled());
        //quorumPeer.setQuorumPeers(config.getAllMembers());
        quorumPeer.setElectionType(config.getElectionAlg());
        quorumPeer.setMyid(config.getServerId());
        quorumPeer.setTickTime(config.getTickTime());
        quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
        quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
        quorumPeer.setInitLimit(config.getInitLimit());
        quorumPeer.setSyncLimit(config.getSyncLimit());
        quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
        quorumPeer.setQuorumVerifier(config.getQuorumVerifier(), false);
        if (config.getLastSeenQuorumVerifier()!=null) {
            quorumPeer.setLastSeenQuorumVerifier(config.getLastSeenQuorumVerifier(), false);
        }
        quorumPeer.initConfigInZKDatabase();
        quorumPeer.setCnxnFactory(cnxnFactory);
        quorumPeer.setSecureCnxnFactory(secureCnxnFactory);
        quorumPeer.setLearnerType(config.getPeerType());
        quorumPeer.setSyncEnabled(config.getSyncEnabled());
        quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
    }

    public void start() {
        quorumPeer.start();
    }

    public void stop() {
        quorumPeer.shutdown();
        try {
            quorumPeer.join();
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Quorum Peer interrupted", e);
        }
    }

}
