package com.kkb.fofN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FReducer extends Reducer<FBean, IntWritable, FBean, IntWritable> {

    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        intWritable = new IntWritable();
    }

    @Override
    protected void reduce(FBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        boolean flag = false;
        for (IntWritable v : values) {
            sum += v.get();
            if (key.getRelations() == 0) {
                flag = true;
            }
        }
        if (!flag) {
            intWritable.set(sum);
            context.write(key, intWritable);
        }
    }
}
