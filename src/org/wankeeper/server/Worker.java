package org.wankeeper.server;

import org.apache.jute.*;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.MultiResponse;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.OpResult.CheckResult;
import org.apache.zookeeper.OpResult.CreateResult;
import org.apache.zookeeper.OpResult.DeleteResult;
import org.apache.zookeeper.OpResult.ErrorResult;
import org.apache.zookeeper.OpResult.SetDataResult;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.quorum.QuorumZooKeeperServer;
import org.apache.zookeeper.txn.TxnHeader;
import org.apache.zookeeper.server.quorum.LearnerHandler;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.wankeeper.message.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wankeeper.token.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.wankeeper.server.Master.HELLO;
import static org.wankeeper.server.Master.PING;
import static org.wankeeper.server.Master.REQUEST;
import static org.wankeeper.server.Master.REPLY;
import static org.wankeeper.server.Master.REPLICATE;
import static org.wankeeper.server.Master.TOKEN;

public class Worker extends ZooKeeperCriticalThread implements Replicator {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

    //private final TokenManagerWorker tokenManager;
    private final RWTokenManager tokenManager;
    private final WanQuorumPeer self;
    protected final ZooKeeperServer zk;
    //protected final LeaderWanKeeperServer leader;
    //protected final WanKeeperServer wzk;
    private BufferedOutputStream bufferedOutputStream;
    private Socket socket;
    private InputArchive in;
    private OutputArchive out;

    public Worker(WanQuorumPeer self, LeaderWanKeeperServer leader) {
        super("Worker", leader.getZooKeeperServerListener());
        this.self = self;
        this.zk = leader;
        this.tokenManager = new RWTokenManagerWorker(this);
    }

    public Worker(WanKeeperServer wzk) {
        super("Worker", wzk.getZooKeeperServerListener());
        this.zk = wzk;
        this.self = null;
        this.tokenManager = new RWTokenManagerWorker(this);
    }

    private void connect() throws IOException, InterruptedException {
        String addr = Config.instance().getMasterAddress();
        int port = Config.instance().getMasterPort();
        InetSocketAddress address = new InetSocketAddress(addr, port);
        socket = new Socket();
        for (int tries = 0; tries < 5; tries++) {
            try {
                socket.connect(address);
                socket.setTcpNoDelay(true);
                LOG.info("Connected to the Master");
                break;
            } catch (IOException e) {
                if (tries >= 4) {
                    LOG.error("Unexpected exception, retries exceeded. tries=" + tries + ", connecting to " + address, e);
                    throw e;
                } else {
                    LOG.error("Unexpected exception, retries exceeded. tries=" + tries + ", connecting to " + address, e);
                    socket = new Socket();
                }
            }
            Thread.sleep(1000);
        }
        in = BinaryInputArchive.getArchive(new BufferedInputStream(socket.getInputStream()));
        bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
        out = BinaryOutputArchive.getArchive(bufferedOutputStream);
    }

    private void registerWithMaster() throws IOException {
        int cid = Config.instance().getClusterID();
        byte[] data = ByteBuffer.allocate(4).putInt(cid).array();
        Packet packet = new Packet(HELLO, data, null);
        writePacket(packet, true);
    }

    private void writePacket(Packet pp, boolean flush) throws IOException {
        synchronized (out) {
            if (pp != null) {
                out.writeRecord(pp, "packet");
            }
            if (flush) {
                bufferedOutputStream.flush();
            }
        }
    }

    private void readPacket(Packet pp) throws IOException {
        synchronized (in) {
            in.readRecord(pp, "packet");
        }
    }

    private final AtomicReference<Request> pending = new AtomicReference<>();

    /**
     * forward request to master (only accessed by RequestProcessorWorker thread)
     * @param request
     * @throws IOException
     */
    public void request(Request request) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oa = new DataOutputStream(baos);
        oa.writeLong(request.sessionId);
        oa.writeInt(request.cxid);
        oa.writeInt(request.type);
        if (request.request != null) {
            request.request.rewind();
            int len = request.request.remaining();
            byte b[] = new byte[len];
            request.request.get(b);
            request.request.rewind();
            oa.write(b);
        }
        oa.close();
        Packet packet = new Packet(REQUEST, baos.toByteArray(), request.authInfo);
        writePacket(packet, true);
        if (Config.instance().isSync()) {
            pending.set(request);
            while (pending.get() != null) {
                LOG.debug("Waiting for request" + request);
                try {
                    synchronized (pending) {
                        pending.wait();
                    }
                } catch (InterruptedException e) {}
            }
        }
    }

    public void send(Packet packet) {
        try {
            writePacket(packet, true);
        } catch (IOException e) {
            LOG.error("Failed to write packet");
            e.printStackTrace();
        }
    }

    public RWTokenManager token() { return tokenManager; }

    private boolean isRunning() {
        if (self != null) {
            return self.isRunning();
        } else if (zk != null) {
            return zk.isRunning();
        }
        LOG.warn("both QuorumPeer and WZK are null");
        return false;
    }

    public void run() {
        try {
            connect();
            registerWithMaster();
            Packet packet = new Packet();
            while (isRunning()) {
                readPacket(packet);
                processPacket(packet);
            }
        } catch (Exception e) {
            LOG.warn("Exception while connecting to the Master", e);
            try { socket.close(); }
            catch (IOException ioe) { }
        }
    }

    private void processPacket(Packet packet) throws IOException {
        switch (packet.getType()) {
            case HELLO:
                // FIXME voting for new master?
                LOG.warn("Not implemented");
                break;
            case PING:
                ping(packet);
                break;
            case REQUEST:
                LOG.debug("Received REQUEST message");
                tokenManager.remove(packet.getTokens());
                break;
            case REPLY: {
                byte[] data = packet.getData();
                long sid = packet.getSid();
                if (data != null) {
                    ReplyHeader header = new ReplyHeader();
                    Record response = SerializeUtils.deserializeResponse(packet.getOp(), data, header);
                    sendResponse(sid, header, response);
                }
                break;
            }
            case REPLICATE: {
                int cid = packet.getCid();
                if (cid == Config.instance().getClusterID()) break;
                byte[] data = packet.getData();
                if (data != null) {
                    TxnHeader header = new TxnHeader();
                    Record record = SerializeUtils.deserializeTxn(data, header);
                    ProcessTxnResult rc = zk.processTxn(header, record);
                    sendResponse(rc, header);

                    if (zk instanceof LeaderWanKeeperServer) {
                        replicate(header.getType(), packet);
                    }
                    if (Config.instance().isSync() && pending.get() != null) {
                        if (header.getClientId() == pending.get().sessionId && header.getCxid() == pending.get().cxid) {
                            synchronized (pending) {
                                LOG.debug("Received replicate for request " + header);
                                pending.set(null);
                                pending.notifyAll();
                            }
                        }

                    }
                }
                break;
            }
            case TOKEN:
                // TODO do we care about from?
                LOG.debug("Received TOKEN: " + packet.getTokens());
                tokenManager.received(packet.getTokens(), packet.getCid());
                break;
            default:
                LOG.warn("Unknown packet type");
                break;
        }
    }

    private void sendResponse(long sid, ReplyHeader header, Record response) {
        for (ServerCnxn cnxn : zk.getServerCnxnFactory().getConnections()) {
            if (sid == cnxn.getSessionId()) {
                zk.decInProcess();
                try {
                    cnxn.sendResponse(header, response, "response");
                } catch (IOException e) {
                    LOG.warn("IOException while sending response", e);
                }
            }
        }
    }

    // Only accessed by worker thread
    private void sendResponse(ProcessTxnResult rc, TxnHeader header) throws IOException {
        String lastOp = "NA";
        KeeperException.Code err = Code.OK;
        Record rsp = null;
        switch (header.getType()) {
            case OpCode.multi: {
                lastOp = "MULT";
                rsp = new MultiResponse() ;
                for (ProcessTxnResult subTxnResult : rc.multiResult) {
                    OpResult subResult ;
                    switch (subTxnResult.type) {
                        case OpCode.check:
                            subResult = new CheckResult();
                            break;
                        case OpCode.create:
                            subResult = new CreateResult(subTxnResult.path);
                            break;
                        case OpCode.create2:
                        case OpCode.createContainer:
                            subResult = new CreateResult(subTxnResult.path, subTxnResult.stat);
                            break;
                        case OpCode.delete:
                        case OpCode.deleteContainer:
                            subResult = new DeleteResult();
                            break;
                        case OpCode.setData:
                            subResult = new SetDataResult(subTxnResult.stat);
                            break;
                        case OpCode.error:
                            subResult = new ErrorResult(subTxnResult.err) ;
                            break;
                        default:
                            throw new IOException("Invalid type of op");
                    }
                    ((MultiResponse)rsp).add(subResult);
                }
                break;
            }
            case OpCode.create: {
                lastOp = "CREA";
                rsp = new CreateResponse(rc.path);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.create2:
            case OpCode.createContainer: {
                lastOp = "CREA";
                rsp = new Create2Response(rc.path, rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.delete:
            case OpCode.deleteContainer: {
                lastOp = "DELE";
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setData: {
                lastOp = "SETD";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.reconfig: {
                lastOp = "RECO";
                rsp = new GetDataResponse(((QuorumZooKeeperServer)zk).self.getQuorumVerifier().toString().getBytes(), rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.setACL: {
                lastOp = "SETA";
                rsp = new SetACLResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
            case OpCode.check: {
                lastOp = "CHEC";
                rsp = new SetDataResponse(rc.stat);
                err = Code.get(rc.err);
                break;
            }
        }

        long lastZxid = zk.getZKDatabase().getDataTreeLastProcessedZxid();
        ReplyHeader hdr = new ReplyHeader(header.getCxid(), lastZxid, err.intValue());

        final long sid = header.getClientId();
        for (ServerCnxn cnxn : zk.getServerCnxnFactory().getConnections()) {
            if (sid == cnxn.getSessionId()) {
//                if (cnxn.getLastCxid() + 1 != header.getCxid()) {
//                    PacketInFlight pending = new PacketInFlight();
//                    pending.header = header;
//                    pending.hdr = hdr;
//                    pending.rsp = rsp;
//                    packetsNotCommitted.add(pending);
//                    break;
//                }
                zk.decInProcess();
                cnxn.updateStatsForResponse(header.getCxid(), lastZxid, lastOp, header.getTime(), Time.currentElapsedTime());
                try {
                    cnxn.sendResponse(hdr, rsp, "response");
//                    for (PacketInFlight pending : packetsNotCommitted) {
//                        if (pending.header.getClientId() == sid && pending.hdr.getXid() == cnxn.getLastCxid() + 1) {
//                            cnxn.updateStatsForResponse(pending.header.getCxid(), lastZxid, lastOp, pending.header.getTime(), Time.currentElapsedTime());
//                            cnxn.sendResponse(pending.hdr, pending.rsp, "response");
//                        }
//                    }
                    break;
                } catch (IOException e) {
                    LOG.warn("IOException while sending response", e);
                }
            }
        }

    }

    @Override
    public void replicate(int type, Packet packet) {
        if (zk instanceof LeaderWanKeeperServer) {
            QuorumPacket quorumPacket = new QuorumPacket(REPLICATE, 0, packet.getData(), packet.getAuthinfo());
            List<LearnerHandler> followers = ((LeaderWanKeeperServer)zk).getLeader().getForwardingFollowers();
            for (LearnerHandler f : followers) {
                f.queuePacket(quorumPacket);
            }
        }
    }

    @Override
    public void replicate(Request request) {
        if (!request.isQuorum()) return;
        if (request.cid != Config.instance().getClusterID()) return;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        try {
            request.getHdr().serialize(boa, "hdr");
            if (request.getTxn() != null) {
                request.getTxn().serialize(boa, "txn");
            }
            baos.close();
        } catch (IOException e) {
            LOG.warn("This really should be impossible", e);
        }
        Packet packet = new Packet(REPLICATE, request.cid, request.sessionId, baos.toByteArray());
        send(packet);
    }

    private void ping(Packet packet) throws IOException {
        // Send back the ping with our session data
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        // TODO should get snpshot
        // Map<Long, Integer> touchTable = zk.getSessionTracker().getTouchSnapshot();
        Map<Long, Integer> touchTable = zk.getZKDatabase().getSessionWithTimeOuts();
        for (Map.Entry<Long, Integer> entry : touchTable.entrySet()) {
            dos.writeLong(entry.getKey());
            dos.writeInt(entry.getValue());
        }
        packet.setData(bos.toByteArray());
        writePacket(packet, true);
    }

}
