package com.kkb.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyWordCount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        // 获取Job对象，组装八个步骤，每个步骤都是一个class类
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration, "mrdemo1");
        // 如果要打包到集群上，必须添加以下配置
        job.setJarByClass(MyWordCount.class);
        // 第一步：读取文件，解析成key, value对，k1行偏移量，v1一行文本内容
        job.setInputFormatClass(TextInputFormat.class);
        // 指定我们去哪一个路径读取文件
        TextInputFormat.addInputPath(job, new Path("file:///d:\\360\\1.txt"));
        // 第二步， 自定义map逻辑，接受k1， v1转换成为新的k2, v2的类型
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 第三到六步 分区，排序，规约，分组 省略
        // 第七步 自定义reduce逻辑
        job.setReducerClass(MyReducer.class);
        // 设置k3， v3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 第八步 输出k3 v3进行保存
        job.setOutputFormatClass(TextOutputFormat.class);
        // 输出路径不可存在
        TextOutputFormat.setOutputPath(job, new Path("file:///d:\\360\\out_result"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("hello", "word");
        configuration.set("mapreduce.framework.name", "local");
        configuration.set("yarn.resourcemanager.hostname", "local");
        int run = ToolRunner.run(configuration, new MyWordCount(), args);
        System.exit(run);
    }
}
