package org.wankeeper.server;

import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.LeaderSessionTracker;
import org.apache.zookeeper.server.quorum.UpgradeableSessionTracker;

import java.io.IOException;

/**
 *  setup a new request processor chain for standalone zookeeper server
 */
public class WanKeeperServer extends ZooKeeperServer {

    private final int cid = Config.instance().getClusterID();
    private final boolean isMaster = Config.instance().isMaster();
    private volatile Master master;
    private volatile Worker worker;

    public WanKeeperServer(FileTxnSnapLog txnLogFactory, int tickTime, int minSessionTimeout, int maxSessionTimeout, ZKDatabase zkDb) {
        super(txnLogFactory, tickTime, minSessionTimeout, maxSessionTimeout, zkDb);
        if (isMaster) {
            try {
                master = new Master(this);
            } catch (IOException e) {
                LOG.error("Fail to create Master");
                shutdown();
            }
        } else {
            worker = new Worker(this);
        }
    }

    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this, isMaster ? master : worker);
        RequestProcessor syncProcessor = new SyncRequestProcessor(this, finalProcessor);
        ((SyncRequestProcessor)syncProcessor).start();
        PrepRequestProcessor prepRequestProcessor = new PrepRequestProcessor(this, syncProcessor);
        prepRequestProcessor.start();
        if (isMaster) {
            firstProcessor = new RequestProcessorMaster(this, prepRequestProcessor, master);
            ((RequestProcessorMaster)firstProcessor).start();
        } else {
            firstProcessor = new RequestProcessorWorker(this, prepRequestProcessor, worker);
            ((RequestProcessorWorker)firstProcessor).start();
        }
    }

    @Override
    public synchronized void startup() {
        super.startup();
        if (isMaster) {
            master.start();
        } else {
            worker.start();
        }
    }

    // TODO need to test this
    @Override
    public void createSessionTracker() {
        sessionTracker = new LeaderSessionTracker(
                this, getZKDatabase().getSessionWithTimeOuts(),
                tickTime, this.getServerId(), false,
                getZooKeeperServerListener());
    }

    @Override
    protected void startSessionTracker() {
        ((UpgradeableSessionTracker) sessionTracker).start();
    }

    public boolean touch(long sess, int to) {
        return sessionTracker.touchSession(sess, to);
    }

}
