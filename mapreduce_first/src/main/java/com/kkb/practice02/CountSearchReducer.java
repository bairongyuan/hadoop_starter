package com.kkb.practice02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountSearchReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable v : values) {
            sum += v.get();
        }
        intWritable.set(sum);
        context.write(key, intWritable);
    }
}
