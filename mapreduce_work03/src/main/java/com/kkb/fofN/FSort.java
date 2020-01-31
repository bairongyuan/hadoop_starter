package com.kkb.fofN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FSort extends WritableComparator {
    public FSort() {
        super(FBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        FBean f1 = (FBean) a;
        FBean f2 = (FBean) b;
        int compareMaster = f1.getMaster().compareTo(f1.getMaster());
        if (compareMaster == 0) {
            int compareRelations = Integer.compare(f1.getRelations(), f2.getRelations());
            if (compareRelations == 0) {
                return f1.getFriend().compareTo(f2.getFriend());
            }
            return -compareRelations;
        }
        return compareMaster;
    }
}
