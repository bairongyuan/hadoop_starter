package com.kkb.fofN;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FBean implements WritableComparable<FBean> {
    private String master;
    private String friend;
    private int relations;

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getFriend() {
        return friend;
    }

    public void setFriend(String friend) {
        this.friend = friend;
    }

    public int getRelations() {
        return relations;
    }

    public void setRelations(int relations) {
        this.relations = relations;
    }

    @Override
    public int compareTo(FBean o) {
        int compareMaster = this.master.compareTo(o.master);
        if (compareMaster == 0) {
            return this.friend.compareTo(o.friend);
        }
        return compareMaster;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(master);
        out.writeUTF(friend);
        out.writeInt(relations);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        master = in.readUTF();
        friend = in.readUTF();
        relations = in.readInt();
    }

    @Override
    public String toString() {
        return master + '\t' + friend + '\t' + relations;
    }
}
