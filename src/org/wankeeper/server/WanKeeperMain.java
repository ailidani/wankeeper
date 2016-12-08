package org.wankeeper.server;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class WanKeeperMain {

    private static final Logger LOG = LoggerFactory.getLogger(WanKeeperMain.class);

    public static void main(String[] args) {
        try {

            (new WanKeeperMain()).start();

        } catch (IOException e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (AdminServerException e) {
            LOG.error("Unable to start AdminServer, exiting abnormally", e);
            System.err.println("Unable to start AdminServer, exiting abnormally");
            System.exit(4);
        }
    }

    public void start() throws IOException, ConfigException, AdminServerException {
        final QuorumPeerConfig config = getQuorumPeerConfig();
        final ServerCnxnFactory cnxnFactory = getConnectionFactory(config);
        final ServerCnxnFactory secureCnxnFactory = getSecureCnxnFactory(config);
        final FileTxnSnapLog fileTxnSnapLog = getFileTxnSnapLog(config);
        final int servers = config.getServers().size();
        if (servers > 0) {
            LOG.info("Starting a quorum peer for a total number of %d servers!", servers);
            ManagedQuorumPeer peer = new ManagedQuorumPeer(config, cnxnFactory, secureCnxnFactory, fileTxnSnapLog);
            peer.start();
        } else {
            LOG.info("Starting a standalone instance!");
            ManagedStandalone standalone = new ManagedStandalone(config, cnxnFactory, secureCnxnFactory, fileTxnSnapLog);
            standalone.start();
        }
    }

    private QuorumPeerConfig getQuorumPeerConfig() throws IOException, ConfigException {
        final QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(Config.get());
        return quorumPeerConfig;
    }

    private ServerCnxnFactory getConnectionFactory(final QuorumPeerConfig config) throws IOException {
        ServerCnxnFactory cnxnFactory = null;
        if (config.getClientPortAddress() != null) {
            cnxnFactory = ServerCnxnFactory.createFactory();
            cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns(), false);
        }
        return cnxnFactory;
    }

    private ServerCnxnFactory getSecureCnxnFactory(final QuorumPeerConfig config) throws IOException {
        ServerCnxnFactory secureCnxnFactory = null;
        if (config.getSecureClientPortAddress() != null) {
            secureCnxnFactory = ServerCnxnFactory.createFactory();
            secureCnxnFactory.configure(config.getSecureClientPortAddress(), config.getMaxClientCnxns(), true);
        }
        return secureCnxnFactory;
    }

    private FileTxnSnapLog getFileTxnSnapLog(final QuorumPeerConfig config) throws IOException {
        return new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir());
    }

}
