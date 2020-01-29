package com.kkb.mr01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class MyPartitioner extends Partitioner<Text, IntWritable> {
    private static HashMap<String, Integer> map = new HashMap<>();
    static {
        map.put("Dear", 0);
        map.put("Bear", 1);
        map.put("River", 2);
        map.put("Car", 3);
    }
    @Override
    public int getPartition(Text text, IntWritable intWritable, int reduceNum) {
        Integer partition = map.get(text.toString());
        return partition;
    }
}
