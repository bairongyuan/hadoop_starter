package com.kkb.demo2_topN;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupPartition extends Partitioner<OrderBean, DoubleWritable> {
    @Override
    public int getPartition(OrderBean orderBean, DoubleWritable doubleWritable, int numReduceTask) {
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTask;
    }
}
