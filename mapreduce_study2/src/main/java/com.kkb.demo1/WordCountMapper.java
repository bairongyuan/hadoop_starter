package com.kkb.demo1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text;
    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
        intWritable = new IntWritable();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        intWritable.set(1);
        for (String word :split) {
            text.set(word);
            context.write(text, intWritable);
        }
    }
}
