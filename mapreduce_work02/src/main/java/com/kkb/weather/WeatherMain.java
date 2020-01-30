package com.kkb.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeatherMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), WeatherMain.class.getSimpleName());

        job.setJarByClass(WeatherMain.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///D:\\360\\practice\\weather\\weather.txt"));
//        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapperClass(WeatherMapper.class);
        job.setMapOutputKeyClass(WeatherBean.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(WeatherPartitioner.class);

        job.setSortComparatorClass(WeatherSortComparator.class);

        job.setGroupingComparatorClass(WeatherGroupCompearator.class);

        job.setReducerClass(WeatherReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///D:\\360\\practice\\weather_out4"));
//        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new WeatherMain(), args);
        System.exit(run);
    }
}
