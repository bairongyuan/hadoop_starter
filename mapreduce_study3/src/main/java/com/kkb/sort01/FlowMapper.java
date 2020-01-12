package com.kkb.sort01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {

    private FlowBean flowBean;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String phoneNum = split[0];
        String upFlow = split[1];
        String downFlow = split[2];
        String upCountFlow = split[3];
        String downCountFlow = split[4];

        flowBean.setPhone(phoneNum);
        flowBean.setUpFlow(Integer.parseInt(upFlow));
        flowBean.setDownFlow(Integer.parseInt(downFlow));
        flowBean.setUpCountFlow(Integer.parseInt(upCountFlow));
        flowBean.setDownCountFlow(Integer.parseInt(downCountFlow));
        context.write(flowBean, NullWritable.get());
    }
}
