package org.wankeeper.message;

import org.apache.jute.*;
import org.apache.zookeeper.data.Id;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PacketOld implements Record {

    private int type;
    private int cid;
    private byte[] data;
    private List<String> paths;
    private List<Id> authinfo;

    public PacketOld() {}

    public PacketOld(int type, List<String> paths) {
        this(type, 0, null, paths, null);
    }

    public PacketOld(int type, byte[] data, List<Id> authinfo) {
        this(type, 0, data, null, authinfo);
    }

    public PacketOld(int type, int cid, byte[] data, List<Id> authinfo) {
        this(type, cid, data, null, authinfo);
    }

    public PacketOld(int type, int cid, byte[] data, List<String> paths, List<Id> authinfo) {
        this.type = type;
        this.cid = cid;
        this.data = data;
        this.paths = paths;
        this.authinfo = authinfo;
    }

    public int getType() {
        return type;
    }

    public PacketOld setType(int type) {
        this.type = type;
        return this;
    }

    public int getCid() {
        return cid;
    }

    public PacketOld setCid(int cid) {
        this.cid = cid;
        return this;
    }

    public byte[] getData() {
        return data;
    }

    public PacketOld setData(byte[] data) {
        this.data = data;
        return this;
    }

    public List<String> getPaths() {
        return paths;
    }

    public PacketOld setPaths(List<String> paths) {
        this.paths = paths;
        return this;
    }

    public List<Id> getAuthinfo() {
        return authinfo;
    }

    public PacketOld setAuthinfo(List<Id> authinfo) {
        this.authinfo = authinfo;
        return this;
    }

    @Override
    public void serialize(OutputArchive archive, String tag) throws IOException {
        archive.startRecord(this, tag);
        archive.writeInt(type, "type");
        archive.writeInt(cid, "cid");
        archive.writeBuffer(data, "data");
        {
            archive.startVector(paths, "paths");
            if (paths != null) {
                for (String path : paths)
                    archive.writeString(path, "path");
            }
            archive.endVector(paths, "paths");
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
        data = archive.readBuffer("data");
        {
            Index index = archive.startVector("paths");
            if (index != null) {
                paths = new ArrayList<>();
                for (; !index.done(); index.incr()) {
                    paths.add(archive.readString("path"));
                }
            }
            archive.endVector("paths");
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
