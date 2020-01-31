package com.kkb.fofN;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), FMain.class.getSimpleName());
        job.setJarByClass(FMain.class);

        FileInputFormat.addInputPath(job, new Path("file:///D:\\360\\practice\\friend\\friend.txt"));

        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(FBean.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(FSort.class);

        job.setReducerClass(FReducer.class);
        job.setOutputKeyClass(FBean.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, new Path("file:///D:\\360\\practice\\friend_outN5"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new FMain(), args);
        System.exit(run);
    }
}
