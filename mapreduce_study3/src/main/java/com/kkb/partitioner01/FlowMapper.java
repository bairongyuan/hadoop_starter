package com.kkb.partitioner01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text text;
    private FlowBean flowBean;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
        flowBean = new FlowBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String phoneNum = split[1];
        String upFlow = split[6];
        String downFlow = split[7];
        String upCountFlow = split[8];
        String downCountFlow = split[9];

        text.set(phoneNum);
        flowBean.setUpFlow(Integer.parseInt(upFlow));
        flowBean.setDownFlow(Integer.parseInt(downFlow));
        flowBean.setUpCountFlow(Integer.parseInt(upCountFlow));
        flowBean.setDownCountFlow(Integer.parseInt(downCountFlow));
        context.write(text, flowBean);
    }
}
