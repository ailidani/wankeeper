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

import static org.wankeeper.token.TokenType.WRITE;
import static org.wankeeper.token.TokenType.READ;

public class RequestProcessorWorker extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RequestProcessorWorker.class);

    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();
    private final ZooKeeperServer zk;
    private final RequestProcessor nextProcessor;
    private final Worker worker;

    public RequestProcessorWorker(ZooKeeperServer zk, RequestProcessor nextProcessor, Worker worker) {
        super("RequestProcessorWorker", zk.getZooKeeperServerListener());
        this.zk = zk;
        this.nextProcessor = nextProcessor;
        this.worker = worker;
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

    private void p(Request request, String path) throws RequestProcessorException, IOException {
        TokenType type = request.isQuorum() ? WRITE : READ;
        if (worker.token().contains(path, type)) {
            LOG.debug("Has " + type.name() + " token: " + path);
            request.cid = Config.instance().getClusterID();
            nextProcessor.processRequest(request);
        } else {
            LOG.debug("Sending " + type.name() + " request to Master for: " + path);
            worker.request(request);
        }
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
            case OpCode.deleteContainer:
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
                if (worker.token().contains(tokens)) {
                    request.cid = cid;
                    nextProcessor.processRequest(request);
                } else {
                    // Send all tokens first
                    // TODO send holding tokens
                    worker.token().remove(tokens);
                    worker.request(request);
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

}
