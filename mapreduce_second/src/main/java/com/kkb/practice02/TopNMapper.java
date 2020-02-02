package com.kkb.practice02;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopNMapper extends Mapper<LongWritable, Text, OrderBean, DoubleWritable> {
    private OrderBean orderBean;
    private DoubleWritable doubleWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        orderBean = new  OrderBean();
        doubleWritable = new DoubleWritable();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
//        System.out.println(value.toString());
        if (split.length == 6) {
            orderBean.setUserId(split[0]);
            String yearMonth = getYearMonth(split[1]);
//            if (StringUtils.isEmpty(yearMonth)){
//                return;
//            }
            orderBean.setDateTime(yearMonth);
            orderBean.setTitle(split[2]);
            orderBean.setUnitPrice(Double.parseDouble(split[3]));
            orderBean.setPurchaseNum(Integer.parseInt(split[4]));
            orderBean.setProduceId(split[5]);
            doubleWritable.set(Double.parseDouble(split[3]) * Integer.parseInt(split[4]));
            context.write(orderBean, doubleWritable);
        }
    }

    private String getYearMonth(String s) {
        String date = s.split(" ")[0];
        String[] split = date.split("-");
        if (split.length>1) {
            String year = split[0];
            String month = split[1];
            return year + "-" + month;
        }
        return "";
    }
}
