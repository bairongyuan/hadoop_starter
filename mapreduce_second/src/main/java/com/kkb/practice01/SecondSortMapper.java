package com.kkb.practice01;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondSortMapper extends Mapper<LongWritable, Text, PersonBean, NullWritable> {
    private PersonBean personBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        personBean = new PersonBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String name = split[0];
        int age = Integer.parseInt(split[1]);
        int salary = Integer.parseInt(split[2]);
        personBean.setName(name);
        personBean.setAge(age);
        personBean.setSalary(salary);

        context.write(personBean, NullWritable.get());
    }
}
