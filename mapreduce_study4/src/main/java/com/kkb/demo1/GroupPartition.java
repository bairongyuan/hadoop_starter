package com.kkb.demo1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupPartition extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTask) {
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTask;
    }
}
