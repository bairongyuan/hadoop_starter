package com.kkb.mr04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        conf.set("key.value.separator.in.input.line", "@zolen@");
        Job job = Job.getInstance(conf, "keyValueDemo");

        job.setJarByClass(WordCountMain.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path("file:///d:\\360\\zolen.txt"));
//        TextInputFormat.addInputPath(job, new Path("file:///d:\\360\\1.txt"));
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///d:\\360\\kv_out"));

        job.setNumReduceTasks(3);

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("hello", "world");
        int run = ToolRunner.run(configuration, new WordCountMain(), args);
        System.exit(run);
    }
}
