package com.kkb.practice02;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TopNRecuder extends Reducer<OrderBean, DoubleWritable, Text, DoubleWritable> {
    private Text text;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
    }

    @Override
    protected void reduce(OrderBean key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (DoubleWritable v : values) {
            if (count < 2) {
                text.set(key.getUserId() + " " + key.getDateTime());
                context.write(text, v);
                count++;
            }
        }
    }
}
