package com.kkb.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text text;
    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
        intWritable = new IntWritable();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // tom hello hadoop cat
        String[] split = value.toString().split(" ");
        for (int i = 1;i < split.length;i++) {
            String outKey = getOutKey(split[0], split[i]);
            text.set(outKey);
            intWritable.set(0);
            context.write(text, intWritable);
            for (int j = i + 1;j < split.length;j++) {
                outKey = getOutKey(split[i], split[j]);
                text.set(outKey);
                intWritable.set(1);
                context.write(text, intWritable);
            }
        }
    }

    private String getOutKey(String s1, String s2) {
        int compare = s1.compareTo(s2);
        if (compare < 0) {
            return s1 + ":" + s2;
        } else {
            return s2 + ":" + s1;
        }
    }
}
