package org.wankeeper.message;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.BadArgumentsException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class Message implements Externalizable {

    public enum Type { Request(1), Replicate(2), Token(3);
        int code;
        Type(int code) { this.code = code; }
        public int code() { return code; }
        static public Type fromCode(int code) throws KeeperException {
            switch (code) {
                case 1: return Type.Request;
                case 2: return Type.Replicate;
                case 3: return Type.Token;
                default:
                    throw new BadArgumentsException("invalid flag value to convert to Message Type");
            }
        }
    }
    private Type type;

    public Message(Type type) {
        this.type = type;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(type.code());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        try {
            type = Type.fromCode(in.readInt());
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

}
