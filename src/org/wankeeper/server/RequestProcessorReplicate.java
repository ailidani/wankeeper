package org.wankeeper.server;

import org.apache.jute.Record;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

public class RequestProcessorReplicate extends ZooKeeperThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorReplicate.class);

    private final LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();
    private final ZooKeeperServer zk;
    private final Replicator replicator;

    public RequestProcessorReplicate(ZooKeeperServer zk, Replicator replicator) {
        super("Replicator");
        this.zk = zk;
        this.replicator = replicator;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Request request = submittedRequests.take();
                if (request.request != null) {
                    TxnHeader header = new TxnHeader();
                    Record record = SerializeUtils.deserializeTxn(request.request.array(), header);
                    zk.processTxn(header, record);
                    if (zk instanceof LeaderWanKeeperServer) {
                        replicator.replicate(request);
                    }
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
                LOG.warn("IOException while deserialize replicate request", e);
            }
        }
    }

    @Override
    public void processRequest(Request request) {
        submittedRequests.add(request);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");
    }

}
