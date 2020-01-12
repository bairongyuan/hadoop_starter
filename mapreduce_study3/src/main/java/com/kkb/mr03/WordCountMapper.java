package com.kkb.mr03;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private IntWritable intWritable;
    private Text text;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable(1);
        text = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        for (String word : split) {
            text.set(word);
            context.write(text, intWritable);
        }
    }
}
