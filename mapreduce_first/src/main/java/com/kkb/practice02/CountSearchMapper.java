package com.kkb.practice02;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountSearchMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text;
    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
        intWritable = new IntWritable(1);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        text.set(split[1]);

        context.write(text, intWritable);
    }
}
