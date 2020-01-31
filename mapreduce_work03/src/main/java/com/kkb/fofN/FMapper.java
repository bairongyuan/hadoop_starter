package com.kkb.fofN;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FMapper extends Mapper<LongWritable, Text, FBean, IntWritable> {

    private FBean fBean;
    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        fBean = new FBean();
        intWritable = new IntWritable();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for (int i = 1;i < split.length;i++) {
            getOutKey(split[0], split[i]);
            fBean.setRelations(0);
            intWritable.set(0);
            context.write(fBean, intWritable);
            for (int j = i + 1;j < split.length;j++) {
                getOutKey(split[i], split[j]);
                fBean.setRelations(1);
                intWritable.set(1);
                context.write(fBean, intWritable);
            }
        }
    }

    private FBean getOutKey(String s1, String s2) {
        int compare = s1.compareTo(s2);
        if (compare < 0) {
            fBean.setMaster(s1);
            fBean.setFriend(s2);
        } else {
            fBean.setMaster(s2);
            fBean.setFriend(s1);
        }
        return fBean;
    }
}
