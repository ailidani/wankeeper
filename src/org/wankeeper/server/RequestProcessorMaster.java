package org.wankeeper.server;

import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.proto.*;
import org.apache.zookeeper.server.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wankeeper.token.TokenType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.wankeeper.token.TokenType.READ;
import static org.wankeeper.token.TokenType.WRITE;

public class RequestProcessorMaster extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorMaster.class);

    private final LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();
    private final ZooKeeperServer zk;
    private final RequestProcessor nextProcessor;
    private final Master master;

    public RequestProcessorMaster(ZooKeeperServer zk, RequestProcessor nextProcessor, Master master) {
        super("RequestProcessorMaster", zk.getZooKeeperServerListener());
        this.zk = zk;
        this.nextProcessor = nextProcessor;
        this.master = master;
//        Blocker blocker = new Blocker();
//        blocker.start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Request request = submittedRequests.take();
                process(request);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (RequestProcessorException e) {
                LOG.warn("Request processor exception");
                e.printStackTrace();
            } catch (IOException e) {
                LOG.warn("IOException in RequestProcessorMaster");
                e.printStackTrace();
            }
        }
    }

    private void p(Request request, String path) throws RequestProcessorException {
        TokenType type = request.isQuorum() ? WRITE : READ;
        if (master.token().hit(path, type, request.from)) {
            request.sending = new TreeMap<>();
            request.sending.put(path, type);
        }
        if (master.token().contains(path, type)) {
            request.cid = Config.instance().getClusterID();
            nextProcessor.processRequest(request);
        } else {
            master.token().get(path, type);
            nextProcessor.processRequest(request);
        }
//        if (!master.hasToken(path)) {
//            master.getTokenAsync(path);
//        }
//        blocking.add(new Tuple(request, path));
    }

    private void process(Request request) throws IOException, RequestProcessorException {
        int cid = Config.instance().getClusterID();
        String path;
        ByteBuffer bb;
        switch (request.type) {
            case OpCode.create:
            case OpCode.create2:
            case OpCode.createContainer:
                CreateRequest createRequest = new CreateRequest();
                bb = request.request.duplicate();
                ByteBufferInputStream.byteBuffer2Record(bb, createRequest);
                path = createRequest.getPath();
                p(request, path);
                break;
            case OpCode.delete:
                DeleteRequest deleteRequest = new DeleteRequest();
                bb = request.request.duplicate();
                ByteBufferInputStream.byteBuffer2Record(bb, deleteRequest);
                path = deleteRequest.getPath();
                p(request, path);
                break;
            case OpCode.setData:
                SetDataRequest setDataRequest = new SetDataRequest();
                bb = request.request.duplicate();
                ByteBufferInputStream.byteBuffer2Record(bb, setDataRequest);
                path = setDataRequest.getPath();
                p(request, path);
                break;
            case OpCode.setACL:
                SetACLRequest setACLRequest = new SetACLRequest();
                bb = request.request.duplicate();
                ByteBufferInputStream.byteBuffer2Record(bb, setACLRequest);
                path = setACLRequest.getPath();
                p(request, path);
                break;
            case OpCode.exists:
                if (!Config.instance().isRWToken()) {
                    request.cid = cid;
                    nextProcessor.processRequest(request);
                } else {
                    ExistsRequest existsRequest = new ExistsRequest();
                    bb = request.request.duplicate();
                    ByteBufferInputStream.byteBuffer2Record(bb, existsRequest);
                    path = existsRequest.getPath();
                    p(request, path);
                }
                break;
            case OpCode.getACL:
                if (!Config.instance().isRWToken()) {
                    request.cid = cid;
                    nextProcessor.processRequest(request);
                } else {
                    GetACLRequest getACLRequest = new GetACLRequest();
                    bb = request.request.duplicate();
                    ByteBufferInputStream.byteBuffer2Record(bb, getACLRequest);
                    path = getACLRequest.getPath();
                    p(request, path);
                }
                break;
            case OpCode.getChildren:
                if (!Config.instance().isRWToken()) {
                    request.cid = cid;
                    nextProcessor.processRequest(request);
                } else {
                    GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                    bb = request.request.duplicate();
                    ByteBufferInputStream.byteBuffer2Record(bb, getChildrenRequest);
                    path = getChildrenRequest.getPath();
                    p(request, path);
                }
                break;
            case OpCode.getChildren2:
                if (!Config.instance().isRWToken()) {
                    request.cid = cid;
                    nextProcessor.processRequest(request);
                } else {
                    GetChildren2Request getChildren2Request = new GetChildren2Request();
                    bb = request.request.duplicate();
                    ByteBufferInputStream.byteBuffer2Record(bb, getChildren2Request);
                    path = getChildren2Request.getPath();
                    p(request, path);
                }
                break;
            case OpCode.getData:
                if (!Config.instance().isRWToken()) {
                    request.cid = cid;
                    nextProcessor.processRequest(request);
                } else {
                    GetDataRequest getDataRequest = new GetDataRequest();
                    bb = request.request.duplicate();
                    ByteBufferInputStream.byteBuffer2Record(bb, getDataRequest);
                    path = getDataRequest.getPath();
                    p(request, path);
                }
                break;

            case OpCode.multi:
                MultiTransactionRecord multiRequest = new MultiTransactionRecord();
                bb = request.request.duplicate();
                ByteBufferInputStream.byteBuffer2Record(bb, multiRequest);
                Map<String, TokenType> tokens = new TreeMap<>();
                for (Op op : multiRequest) {
                    switch (op.getType()) {
                        case OpCode.create:
                        case OpCode.create2:
                        case OpCode.createContainer:
                        case OpCode.setData:
                        case OpCode.setACL:
                        case OpCode.delete:
                        case OpCode.deleteContainer:
                            tokens.put(op.getPath(), WRITE);
                            break;
                        case OpCode.exists:
                        case OpCode.getACL:
                        case OpCode.getChildren:
                        case OpCode.getChildren2:
                        case OpCode.getData:
                            if (Config.instance().isRWToken()) {
                                tokens.put(op.getPath(), READ);
                            } else {
                                tokens.put(op.getPath(), WRITE);
                            }
                            break;
                        default:
                            LOG.warn("Unknown Op type");
                            break;
                    }
                }
                for (Map.Entry<String, TokenType> entry : tokens.entrySet()) {
                    if (master.token().hit(entry.getKey(), entry.getValue(), request.from)) {
                        if (request.sending == null) request.sending = new TreeMap<>();
                        request.sending.put(entry.getKey(), entry.getValue());
                    }
                }
                if (master.token().contains(tokens)) {
                    request.cid = cid;
                    nextProcessor.processRequest(request);
                } else {
                    master.token().get(tokens);
                    nextProcessor.processRequest(request);
                }
                break;
            default:
                request.cid = cid;
                nextProcessor.processRequest(request);
                break;
        }

    }

    @Override
    public void processRequest(Request request) {
        submittedRequests.add(request);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutting down");
        submittedRequests.clear();
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }

    private static class Tuple {
        Request request;
        String path;
        Tuple(Request request, String path) {
            this.request = request;
            this.path = path;
        }
    }

    private LinkedBlockingQueue<Tuple> blocking = new LinkedBlockingQueue<>();
    class Blocker extends Thread {
        @Override
        public void run() {
            while(true) {
                try {
                    Tuple tuple = blocking.take();
                    if (master.token().contains(tuple.path, WRITE)) {
                        tuple.request.cid = Config.instance().getClusterID();
                        nextProcessor.processRequest(tuple.request);
                    } else {
                        master.token().contains(tuple.path, WRITE);
                        nextProcessor.processRequest(tuple.request);
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (RequestProcessorException e) {
                    LOG.warn("Request processor exception");
                    e.printStackTrace();
                }

            }
        }
    }

}
