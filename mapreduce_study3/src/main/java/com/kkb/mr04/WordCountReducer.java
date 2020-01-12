package com.kkb.mr04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable outputValue;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        outputValue = new IntWritable();
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : values) {
            sum += intWritable.get();
        }
        outputValue.set(sum);
        context.write(key, outputValue);
    }
}
