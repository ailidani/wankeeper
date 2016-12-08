package org.wankeeper.server;

import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.PrepRequestProcessor;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.*;
import java.io.IOException;

/**
 *  setup a new request processor chain
 */
public class LeaderWanKeeperServer extends LeaderZooKeeperServer {

    private final int cid = Config.instance().getClusterID();
    private final boolean isMaster = Config.instance().isMaster();
    private volatile Master master;
    private volatile Worker worker;

    public LeaderWanKeeperServer(FileTxnSnapLog logFactory, WanQuorumPeer self, ZKDatabase zkDb) throws IOException {
        super(logFactory, self, zkDb);
        if (isMaster) {
            master = new Master(self, this);
        } else {
            worker = new Worker(self, this);
        }
    }

    @Override
    protected void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(this, (isMaster ? master : worker));
        RequestProcessor toBeAppliedProcessor = new Leader.ToBeAppliedRequestProcessor(finalProcessor, getLeader());
        commitProcessor = new CommitProcessor(toBeAppliedProcessor, Long.toString(getServerId()), false, getZooKeeperServerListener());
        commitProcessor.start();
        ProposalRequestProcessor proposalProcessor = new ProposalRequestProcessor(this, commitProcessor);
        proposalProcessor.initialize();
        prepRequestProcessor = new PrepRequestProcessor(this, proposalProcessor);
        prepRequestProcessor.start();
        if (isMaster) {
            RequestProcessorMaster requestProcessorMaster = new RequestProcessorMaster(this, prepRequestProcessor, master);
            requestProcessorMaster.start();
            firstProcessor = new LeaderRequestProcessor(this, requestProcessorMaster);
        } else {
            RequestProcessorWorker requestProcessorWorker = new RequestProcessorWorker(this, prepRequestProcessor, worker);
            requestProcessorWorker.start();
            firstProcessor = new LeaderRequestProcessor(this, requestProcessorWorker);
        }

        setupContainerManager();
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

}
