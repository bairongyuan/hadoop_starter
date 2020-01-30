package com.kkb.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class WeatherPartitioner extends Partitioner<WeatherBean, IntWritable> {
    @Override
    public int getPartition(WeatherBean weatherBean, IntWritable intWritable, int numReduceTask) {
        return weatherBean.hashCode() % numReduceTask;
//        return weatherBean.getYear() % numReduceTask;
    }
}
