package com.yc.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//这个案例演示在本地执行，完全不依赖于hdfs和mapreduce集群.
public class App extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int result= ToolRunner.run(   new Configuration(), new App(), args);
        System.exit(result );

    }

    //重写Tool的run方法
    @Override
    public int run(String[] args) throws Exception {
        //父类的方法getConf();
        Configuration config=getConf();
        //获取参数
        /**
         * GenericOptionsParser 命令行解析器
         * 是hadoop框架中解析命令行参数的基本类。它能够辨别一些标准的命令行参数，能够使应用程序轻易地指定namenode，jobtracker，以及其他额外的配置资源
         比如：hadoop dfs -fs master:8020 -ls /data

         GenericOptionsParser把  -fs master:8020配置到配置conf中

         而getRemainingArgs()方法则得到剩余的参数，就是 -ls /data。供下面使用输入输出参数
         */
        String[] otherArgs=new GenericOptionsParser(config,args).getRemainingArgs();

        Job job=new Job(config,"第一个任务:单词计数");
        /**
         * 相当于Job job=Job.getInstance(conf,"没有机场的城市");
         */
        job.setJarByClass(  App.class );

        //方案一: hdfs文件   指定要分析的文件在hdfs的位置，及分析的任务
        //FileInputFormat.addInputPath(job, new Path("/wc/input/wc.txt"));
        //创建 输出 文件，并判断 这个输出 文件是否存在
        //Path outpath=new Path("/wc/output/");
        //FileSystem fs=FileSystem.get(config);
        //if(  fs.exists(outpath)){
        //	fs.delete(outpath, true);
        ///}
        //FileOutputFormat.setOutputPath(job, outpath);

        //方案二: 本地文件按以下设置

        //指定输入输出位置
        FileInputFormat.addInputPath(job, new Path(  otherArgs[0]  ));
        FileOutputFormat.setOutputPath(job, new Path( otherArgs[1]));

        //设定各个类
        job.setMapperClass(WcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WcReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class );

        //true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        job.waitForCompletion(true);

        return job.isSuccessful()?0:1;
    }
}
