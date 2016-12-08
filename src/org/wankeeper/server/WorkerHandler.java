package org.wankeeper.server;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.wankeeper.message.Packet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import static org.wankeeper.server.Master.*;

public class WorkerHandler extends ZooKeeperThread {

    private static final Logger LOG = LoggerFactory.getLogger(WorkerHandler.class);

    private final Socket socket;
    private final Master master;
    private int cid;
    private final LinkedBlockingQueue<Packet> queuedPackets = new LinkedBlockingQueue<>();
    private volatile BinaryInputArchive ia;
    private volatile BinaryOutputArchive oa;
    private volatile BufferedOutputStream bufferedOutput;

    public WorkerHandler(Socket socket, Master master) {
        super("WorkerHandler-" + socket.getRemoteSocketAddress());
        this.socket = socket;
        this.master = master;
    }

    public int getCid() { return cid; }

    private void sendPackets() throws InterruptedException {
        while (true) {
            try {
                Packet p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }
                oa.writeRecord(p, "packet");
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try { socket.close(); }
                    catch (IOException ie) { LOG.warn("Error closing socket for handler " + this, ie); }
                }
                break;
            }
        }
    }

    private volatile boolean sendingThreadStarted = false;

    private void startSendingPackets() {
        if (!sendingThreadStarted) {
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName("MasterSender-" + socket.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption " + e.getMessage());
                    }
                }
            }.start();
            sendingThreadStarted = true;
        } else {
            LOG.error("Attempting to start sending thread after it already started");
        }
    }

    @Override
    public void run() {
        try {
            ia = BinaryInputArchive.getArchive(new BufferedInputStream(socket.getInputStream()));
            bufferedOutput = new BufferedOutputStream(socket.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            Packet packet = new Packet();
            ia.readRecord(packet, "packet");
            if (packet.getType() != HELLO) {
                LOG.error("First packet " + packet.toString() + " is not HELLO");
                return;
            }
            byte[] data = packet.getData();
            if (data != null) {
                ByteBuffer bbcid = ByteBuffer.wrap(data);
                if (data.length >= 4) cid = bbcid.getInt();
            } else {
                cid = -1;
            }
            master.addWorker(cid, this);

            startSendingPackets();

            while (true) {
                packet = new Packet();
                ia.readRecord(packet, "packet");
                ByteBuffer bb;
                switch (packet.getType()) {
                    case PING:
                        // Process the touches
                        ByteArrayInputStream bis = new ByteArrayInputStream(packet.getData());
                        DataInputStream dis = new DataInputStream(bis);
                        while (dis.available() > 0) {
                            long sess = dis.readLong();
                            int to = dis.readInt();
                            if (LOG.isDebugEnabled())
                                LOG.debug("Touch session: " + sess);
                            master.touch(sess, to);
                        }
                        break;
                    case REQUEST:
                        bb = ByteBuffer.wrap(packet.getData());
                        long sessionID = bb.getLong();
                        int cxid = bb.getInt();
                        int type = bb.getInt();
                        bb = bb.slice();
                        Request request = new Request(null, sessionID, cxid, type, bb, packet.getAuthinfo());
                        request.from = cid;
                        if (LOG.isDebugEnabled())
                            LOG.debug("Received request from " + cid + " " + request);
                        master.submitRequest(request);
                        break;
                    case REPLICATE:
                        int cid = packet.getCid();
                        if (cid == Config.instance().getClusterID()) break;
                        byte[] txnBytes = packet.getData();
                        if (txnBytes != null) {
                            TxnHeader header = new TxnHeader();
                            Record record = SerializeUtils.deserializeTxn(txnBytes, header);
                            master.getZooKeeperServer().processTxn(header, record);
                            master.replicate(header.getType(), packet);
                        }
                        break;
                    case TOKEN:
                        master.token().received(packet.getTokens(), getCid());
                        break;
                    default:
                        LOG.warn("Unexpected packet type");
                        break;
                }
            }


        } catch (IOException e) {
            if (socket != null && !socket.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock still open", e);
                //close the socket to make sure the other side can see it being close
                try { socket.close(); } catch(IOException ie) {}
            }
        }
    }

    void queuePacket(Packet p) {
        queuedPackets.add(p);
    }

    public void ping() {
        if (!sendingThreadStarted) {
            return;
        }
        Packet ping = new Packet(Master.PING, null, null);

//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        DataOutputStream dos = new DataOutputStream(bos);
//        // TODO should get snpshot
//        // Map<Long, Integer> touchTable = zk.getSessionTracker().getTouchSnapshot();
//        Map<Long, Integer> touchTable = master.getZooKeeperServer().getZKDatabase().getSessionWithTimeOuts();
//        try {
//            for (Map.Entry<Long, Integer> entry : touchTable.entrySet()) {
//                dos.writeLong(entry.getKey());
//                dos.writeInt(entry.getValue());
//            }
//        } catch (IOException e) {
//            // ignore
//        }
//        ping.setData(bos.toByteArray());

        queuePacket(ping);
    }

}
