package com.kkb.practice02;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TopNGroup extends WritableComparator {
    public TopNGroup() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean a1 = (OrderBean) a;
        OrderBean a2 = (OrderBean) b;
        int compareUserId = a1.getUserId().compareTo(a2.getUserId());
        if (compareUserId == 0) {
            return a1.getDateTime().compareTo(a2.getDateTime());
        }
        return compareUserId;
    }
}
