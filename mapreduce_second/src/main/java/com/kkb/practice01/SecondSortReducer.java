package com.kkb.practice01;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondSortReducer extends Reducer<PersonBean, NullWritable, PersonBean, NullWritable> {
    @Override
    protected void reduce(PersonBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
