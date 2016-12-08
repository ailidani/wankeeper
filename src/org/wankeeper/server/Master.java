package org.wankeeper.server;

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.*;
import org.apache.zookeeper.server.quorum.LearnerHandler;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.KeeperException.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wankeeper.message.Packet;
import org.wankeeper.token.RWTokenManagerMaster;
import org.wankeeper.token.TokenType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Master extends ZooKeeperCriticalThread implements Replicator {

    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    public static final int HELLO = 20;
    public static final int PING = 21;
    public static final int REQUEST = 22;
    public static final int REPLY = 23;
    public static final int REPLICATE = 24;
    public static final int TOKEN = 25;

    static final boolean SSL = Config.instance().ssl();
    static final int PORT = Config.instance().getMasterPort();

    //private final TokenManagerMaster tokenManager;
    private final RWTokenManagerMaster tokenManager;
    private final WanQuorumPeer self;
    protected final ZooKeeperServer zk;
    protected final LeaderWanKeeperServer leader;
    protected final WanKeeperServer wzk;
    private final ServerSocket socket;

    // the work acceptor thread
    private volatile CnxnAcceptor cnxnAcceptor = null;
    private final Map<Integer, WorkerHandler> workers = new ConcurrentHashMap<>();

    public Master(WanQuorumPeer self, LeaderWanKeeperServer leader) throws IOException {
        super("Master", leader.getZooKeeperServerListener());
        this.zk = leader;
        this.self = self;
        this.leader = leader;
        this.wzk = null;
        socket = new ServerSocket(PORT);
        socket.setReuseAddress(true);
        this.tokenManager = new RWTokenManagerMaster(this);
    }

    public Master(WanKeeperServer wzk) throws IOException {
        super("Master", wzk.getZooKeeperServerListener());
        this.zk = wzk;
        this.wzk = wzk;
        this.self = null;
        this.leader = null;
        socket = new ServerSocket(PORT);
        socket.setReuseAddress(true);
        this.tokenManager = new RWTokenManagerMaster(this);
    }

    public void run() {
        cnxnAcceptor = new CnxnAcceptor();
        cnxnAcceptor.start();

        while (true) {
            synchronized (this) {
                long start = Time.currentElapsedTime();
                long cur = start;
                long end = start + Config.instance().getTickTime() / 2;
                while (cur < end) {
                    try {
                        wait(end - cur);
                    } catch (InterruptedException e) {
                    }
                    cur = Time.currentElapsedTime();
                }
            }
            for (WorkerHandler worker : workers.values()) {
                worker.ping();
            }
        }
    }

    public void addWorker(int cid, WorkerHandler worker) {
        workers.put(cid, worker);
    }

    public ZooKeeperServer getZooKeeperServer() { return zk; }

    public void submitRequest(Request request) {
        if (leader != null) {
            leader.submitLearnerRequest(request);
        } else {
            wzk.submitRequest(request);
        }
    }

    public boolean touch(long sess, int to) {
        if (leader != null) {
            return leader.touch(sess, to);
        } else if (wzk != null) {
            return wzk.touch(sess, to);
        }
        return false;
    }

    public RWTokenManagerMaster token() {
        return tokenManager;
    }

    public void send(int cid, Packet packet) {
        workers.get(cid).queuePacket(packet);
    }

    @Override
    public void replicate(Request request) {
        if (request.type == OpCode.createSession || request.type == OpCode.closeSession)
            return;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);

        if (!request.isQuorum() && request.from != Config.instance().getClusterID()) {
            Record rsp = null;
            Code err = Code.OK;
            try {
                switch (request.type) {
                    case OpCode.exists: {
                        ExistsRequest existsRequest = new ExistsRequest();
                        ByteBufferInputStream.byteBuffer2Record(request.request, existsRequest);
                        String path = existsRequest.getPath();
                        if (path.indexOf('\0') != -1) {
                            throw new KeeperException.BadArgumentsException();
                        }
                        Stat stat = zk.getZKDatabase().statNode(path, null);
                        rsp = new ExistsResponse(stat);
                        break;
                    }
                    case OpCode.getData: {
                        GetDataRequest getDataRequest = new GetDataRequest();
                        ByteBufferInputStream.byteBuffer2Record(request.request, getDataRequest);
                        DataNode n = zk.getZKDatabase().getNode(getDataRequest.getPath());
                        if (n == null) {
                            throw new KeeperException.NoNodeException();
                        }
                        Long aclL;
                        synchronized (n) {
                            aclL = n.acl;
                        }
                        PrepRequestProcessor.checkACL(zk, zk.getZKDatabase().convertLong(aclL), ZooDefs.Perms.READ, request.authInfo);
                        Stat stat = new Stat();
                        byte b[] = zk.getZKDatabase().getData(getDataRequest.getPath(), stat, null);
                        rsp = new GetDataResponse(b, stat);
                        break;
                    }
                    case OpCode.getChildren: {
                        GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                        ByteBufferInputStream.byteBuffer2Record(request.request, getChildrenRequest);
                        DataNode n = zk.getZKDatabase().getNode(getChildrenRequest.getPath());
                        if (n == null) {
                            throw new KeeperException.NoNodeException();
                        }
                        Long aclG;
                        synchronized (n) {
                            aclG = n.acl;
                        }
                        PrepRequestProcessor.checkACL(zk, zk.getZKDatabase().convertLong(aclG), ZooDefs.Perms.READ, request.authInfo);
                        List<String> children = zk.getZKDatabase().getChildren(getChildrenRequest.getPath(), null, null);
                        rsp = new GetChildrenResponse(children);
                        break;
                    }
                    case OpCode.getChildren2: {
                        GetChildren2Request getChildren2Request = new GetChildren2Request();
                        ByteBufferInputStream.byteBuffer2Record(request.request, getChildren2Request);
                        Stat stat = new Stat();
                        DataNode n = zk.getZKDatabase().getNode(getChildren2Request.getPath());
                        if (n == null) {
                            throw new KeeperException.NoNodeException();
                        }
                        Long aclG;
                        synchronized (n) {
                            aclG = n.acl;
                        }
                        PrepRequestProcessor.checkACL(zk, zk.getZKDatabase().convertLong(aclG), ZooDefs.Perms.READ, request.authInfo);
                        List<String> children = zk.getZKDatabase().getChildren(getChildren2Request.getPath(), stat, null);
                        rsp = new GetChildren2Response(children, stat);
                    }
                    case OpCode.getACL: {
                        GetACLRequest getACLRequest = new GetACLRequest();
                        ByteBufferInputStream.byteBuffer2Record(request.request, getACLRequest);
                        Stat stat = new Stat();
                        List<ACL> acl = zk.getZKDatabase().getACL(getACLRequest.getPath(), stat);
                        rsp = new GetACLResponse(acl, stat);
                        break;
                    }
                }
            } catch (KeeperException e) {
                err = e.code();
            } catch (IOException e) {
                LOG.error("Failed to process " + request, e);
                StringBuilder sb = new StringBuilder();
                ByteBuffer bb = request.request;
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
                LOG.error("Dumping request buffer: 0x" + sb.toString());
                err = KeeperException.Code.MARSHALLINGERROR;
            }

            long lastZxid = zk.getZKDatabase().getDataTreeLastProcessedZxid();
            ReplyHeader hdr = new ReplyHeader(request.cxid, lastZxid, err.intValue());

            try {
                hdr.serialize(boa, "header");
                if (rsp != null) {
                    rsp.serialize(boa, "response");
                }
                baos.close();
            } catch (IOException e) {
                LOG.warn("reply serialization error");
            }
            Packet packet = new Packet(REPLY, request.cid, request.sessionId, request.type, baos.toByteArray());
            workers.get(request.from).queuePacket(packet);
            if (request.sending != null) {
                LOG.debug("Sending token " + request.sending);
                workers.get(request.from).queuePacket(new Packet(TOKEN, request.sending));
                tokenManager.put(request.sending, request.from);
            }
        }

        if (request.isQuorum()) {
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
            synchronized (workers) {
                for (WorkerHandler worker : workers.values()) {
                    if (request.cid == worker.getCid()) continue;
                    worker.queuePacket(packet);
                    if (request.sending != null && request.from == worker.getCid()) {
                        if (LOG.isDebugEnabled())
                            LOG.debug("Sending token " + request.sending);
                        worker.queuePacket(new Packet(TOKEN, request.sending));
                        // TODO remove from tokens set?????
                        tokenManager.put(request.sending, request.from);
                    }
                }
            }
        }

    }

    @Override
    public void replicate(int type, Packet packet) {
        if (type != ZooDefs.OpCode.createSession && type != ZooDefs.OpCode.closeSession) {
            synchronized (workers) {
                for (WorkerHandler worker : workers.values()) {
                    if (packet.getCid() == worker.getCid()) continue;
                    worker.queuePacket(packet);
                }
            }
        }

        if (zk instanceof LeaderWanKeeperServer) {
            QuorumPacket quorumPacket = new QuorumPacket(REPLICATE, 0, packet.getData(), packet.getAuthinfo());
            List<LearnerHandler> followers = ((LeaderWanKeeperServer)zk).getLeader().getForwardingFollowers();
            for (LearnerHandler f : followers) {
                f.queuePacket(quorumPacket);
            }
        }
    }


    class CnxnAcceptor extends ZooKeeperCriticalThread {

        private volatile boolean stop = false;

        public CnxnAcceptor() {
            super("CnxnAcceptor-" + socket.getLocalSocketAddress(), zk.getZooKeeperServerListener());
        }

        @Override
        public void run() {
            while (!stop) {
                try {
                    Socket cnxn = socket.accept();
                    cnxn.setTcpNoDelay(true);
                    WorkerHandler worker = new WorkerHandler(cnxn, Master.this);
                    worker.start();
                } catch (Exception e) {
                    if (stop) {
                        LOG.info("Exception while shutting down acceptor: ", e);
                        stop = true;
                    } else {
                        LOG.warn("Exception while accepting Worker", e.getMessage());
                        handleException(this.getName(), e);
                    }
                }
            }
        }

        public void halt() {
            stop = true;
        }
    }

}
