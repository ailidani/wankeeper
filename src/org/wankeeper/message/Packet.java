package org.wankeeper.message;

import org.apache.jute.*;
import org.apache.zookeeper.data.Id;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.wankeeper.token.TokenType;


public class Packet implements Record {

    private int type;
    // committed cluster id
    private int cid;
    // client session id
    private long sid;
    // reply for OpCode
    private int op;
    private byte[] data;
    private TreeMap<String, TokenType> tokens;
    private List<Id> authinfo;

    public Packet() {}

    // for TOKEN message
    public Packet(int type, TreeMap<String, TokenType> tokens) {
        this(type, 0, 0, 0, null, tokens, null);
    }

    // for REQUEST message
    public Packet(int type, byte[] data, List<Id> authinfo) {
        this(type, 0, 0, 0, data, null, authinfo);
    }

    // for REPLICATE message
    public Packet(int type, int cid, long sid, byte[] data) {
        this(type, cid, sid, 0, data, null, null);
    }

    // for REPLY message
    public Packet(int type, int cid, long sid, int op, byte[] data) {
        this(type, cid, sid, op, data, null, null);
    }

    public Packet(int type, int cid, long sid, int op, byte[] data, TreeMap<String, TokenType> tokens, List<Id> authinfo) {
        this.type = type;
        this.cid = cid;
        this.sid = sid;
        this.op = op;
        this.data = data;
        this.tokens = tokens;
        this.authinfo = authinfo;
    }

    public int getType() {
        return type;
    }

    public Packet setType(int type) {
        this.type = type;
        return this;
    }

    public int getCid() {
        return cid;
    }

    public Packet setCid(int cid) {
        this.cid = cid;
        return this;
    }

    public long getSid() {
        return sid;
    }

    public Packet setSid(long sid) {
        this.sid = sid;
        return this;
    }

    public int getOp() {
        return op;
    }

    public Packet setOp(int op) {
        this.op = op;
        return this;
    }

    public byte[] getData() {
        return data;
    }

    public Packet setData(byte[] data) {
        this.data = data;
        return this;
    }

    public Map<String, TokenType> getTokens() {
        return tokens;
    }

    public Packet setTokens(TreeMap<String, TokenType> tokens) {
        this.tokens = tokens;
        return this;
    }

    public List<Id> getAuthinfo() {
        return authinfo;
    }

    public Packet setAuthinfo(List<Id> authinfo) {
        this.authinfo = authinfo;
        return this;
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        archive.writeInt(type, "type");
        archive.writeInt(cid, "cid");
        archive.writeLong(sid, "sid");
        archive.writeInt(op, "op");
        archive.writeBuffer(data, "data");
        {
            archive.startMap(tokens, "tokens");
            if (tokens != null) {
                for (Map.Entry<String, TokenType> entry : tokens.entrySet()) {
                    archive.writeString(entry.getKey(), "path");
                    archive.writeInt(entry.getValue().getCode(), "ttype");
                }
            }
            archive.endMap(tokens, "tokens");
        }
        {
            archive.startVector(authinfo, "authinfo");
            if (authinfo != null) {
                int size = authinfo.size();
                for (int i=0; i<size; i++) {
                    archive.writeRecord(authinfo.get(i), "Id");
                }
            }
            archive.endVector(authinfo, "authinfo");
        }
        archive.endRecord(this, tag);
    }

    @Override
    public void deserialize(InputArchive archive, String tag) throws IOException {
        archive.startRecord(tag);
        type = archive.readInt("type");
        cid = archive.readInt("cid");
        sid = archive.readLong("sid");
        op = archive.readInt("op");
        data = archive.readBuffer("data");
        {
            Index index = archive.startMap("tokens");
            if (index != null) {
                tokens = new TreeMap<>();
                for (; !index.done(); index.incr()) {
                    tokens.put(archive.readString("path"), TokenType.fromInt(archive.readInt("ttype")));
                }
            }
            archive.endMap("tokens");
        }
        {
            Index index = archive.startVector("authinfo");
            if (index != null) {
                authinfo = new ArrayList<Id>();
                for (; !index.done(); index.incr()) {
                    Id id = new Id();
                    archive.readRecord(id, "Id");
                    authinfo.add(id);
                }
            }
            archive.endVector("authinfo");
        }
        archive.endRecord(tag);
    }

    public void write(DataOutput out) throws IOException {
        BinaryOutputArchive archive = new BinaryOutputArchive(out);
        serialize(archive, "");
    }

    public void readFields(DataInput in) throws IOException {
        BinaryInputArchive archive = new BinaryInputArchive(in);
        deserialize(archive, "");
    }
}
