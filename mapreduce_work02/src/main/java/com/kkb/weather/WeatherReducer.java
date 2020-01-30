package com.kkb.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<WeatherBean, IntWritable, Text, IntWritable> {
    private Text text;
    private IntWritable intWritable;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
        intWritable = new IntWritable();
    }


    @Override
    protected void reduce(WeatherBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int flag = 0;
        int day = 0;
        for (IntWritable v : values) {
            if (flag<2) {
                text.set(key.getYear() + "-" + key.getMonth() + "-" + key.getDay() + ":" + key.getWd());
                intWritable.set(key.getWd());
                flag++;
                day = key.getDay();
                context.write(text, intWritable);
            } else {
                break;
            }
        }

    }
}
