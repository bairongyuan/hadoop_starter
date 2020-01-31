package com.kkb.practice01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        Counter counter = context.getCounter("DataCounter", "damageCounter");
        if (split.length == 6) {
            context.write(value, NullWritable.get());
        } else {
            counter.increment(1l);
        }
    }
}
