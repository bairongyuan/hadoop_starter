package com.kkb.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class WeatherMapper extends Mapper<LongWritable, Text, WeatherBean, IntWritable> {
    private WeatherBean weatherBean;
    private IntWritable intWritable;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        weatherBean = new WeatherBean();
        intWritable = new IntWritable();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] split = value.toString().split("\t");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(split[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            weatherBean.setYear( calendar.get(Calendar.YEAR));
            weatherBean.setMonth(calendar.get(Calendar.MONTH)+1);
            weatherBean.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            int wd = Integer.parseInt(split[1].substring(0, split[1].length() - 1));
            weatherBean.setWd(wd);
            intWritable.set(wd);
            context.write(weatherBean, intWritable);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}
