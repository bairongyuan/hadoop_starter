package com.kkb.practice01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondSortMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), SecondSortMain.class.getSimpleName());
        job.setJarByClass(SecondSortMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///C:\\Users\\Administrator\\Downloads\\kkb\\课后资料-MR第三次课(1)\\MR练习题\\数据\\salary.txt"));

        job.setMapperClass(SecondSortMapper.class);
        job.setMapOutputKeyClass(PersonBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(SecondSortReducer.class);
        job.setOutputKeyClass(PersonBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///D:\\360\\practice\\SecondSortOut"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new SecondSortMain(), args);
        System.exit(run);
    }
}
