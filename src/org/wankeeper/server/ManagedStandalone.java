package org.wankeeper.server;

import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.admin.AdminServerFactory;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ManagedStandalone {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedStandalone.class);

    private final ZooKeeperServer zooKeeperServer;
    private final AdminServer adminServer;
    // ZooKeeper server supports two kinds of connection: unencrypted and encrypted.
    private final ServerCnxnFactory cnxnFactory;
    private final ServerCnxnFactory secureCnxnFactory;

    public ManagedStandalone(final QuorumPeerConfig config,
                             final ServerCnxnFactory cnxnFactory,
                             final ServerCnxnFactory secureCnxnFactory,
                             final FileTxnSnapLog fileTxnSnapLog) {
        if (Config.instance().getClusterID() == 0) {
            this.zooKeeperServer = new ZooKeeperServer(fileTxnSnapLog, config.getTickTime(), config.getMinSessionTimeout(), config.getMaxSessionTimeout(), null);
        } else {
            this.zooKeeperServer = new WanKeeperServer(fileTxnSnapLog, config.getTickTime(), config.getMinSessionTimeout(), config.getMaxSessionTimeout(), null);
        }
        this.adminServer = AdminServerFactory.createAdminServer();
        this.adminServer.setZooKeeperServer(zooKeeperServer);
        this.cnxnFactory = cnxnFactory;
        this.secureCnxnFactory = secureCnxnFactory;
    }

    public void start() throws AdminServerException, IOException {
        try {
            adminServer.start();
            if (cnxnFactory != null) {
                cnxnFactory.startup(zooKeeperServer, true);
            } else if (secureCnxnFactory != null) {
                secureCnxnFactory.startup(zooKeeperServer);
            }
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Server interrupted", e);
        }
    }

    public void stop() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.shutdown();
        }
        if (zooKeeperServer.isRunning()) {
            zooKeeperServer.shutdown();
        }
        try {
            adminServer.shutdown();
        } catch (AdminServerException e) {
            LOG.warn("Problem stopping AdminServer", e);
        }
    }

}
