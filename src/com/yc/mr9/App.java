package com.yc.mr9;


import com.yc.mr9.bean.Weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * hadoop jar mysecondmapreduce-1.0-SNAPSHOT.jar com.yc.wc1.App /wc1/input/file01.txt /wc/output/
 */


//这个案例演示在本地执行，完全不依赖于hdfs和mapreduce集群.
public class App extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int result= ToolRunner.run(new Configuration(), new App(), args);
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

        Job job=new Job(config,"天气预报");
        job.setJarByClass(  App.class );

        //方案一: hdfs文件   指定要分析的文件在hdfs的位置，及分析的任务
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //创建 输出 文件，并判断 这个输出 文件是否存在
        Path outpath=new Path(otherArgs[1]);
        FileSystem fs=FileSystem.get(config);
        if(fs.exists(outpath)){
            fs.delete(outpath, true);
        }
        FileOutputFormat.setOutputPath(job, outpath);


        //方案二: 本地文件按以下设置

//        //指定输入输出位置
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        //根据这个路径，查看该路径是否存在
//        Path outputPath=new Path(otherArgs[1]);
//        FileSystem fs=FileSystem.get(config);
//        if(fs.exists(outputPath)){
//            fs.delete(outputPath,true);
//        }
//        FileOutputFormat.setOutputPath(job, outputPath);

        //设定各个类
        job.setMapperClass(WeatherMapper.class);//指定Mapper
        job.setMapOutputKeyClass(Weather.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(WeatherReducer.class);//指定规约器类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class );

        job.setSortComparatorClass(WeatherSort.class);//指定排序类
        job.setGroupingComparatorClass(WeatherGroup.class);//指定分组类

        job.setPartitionerClass(WeatherPartition.class);//指定分区类

        //指定reduce任务数，请注意最后在hdfs中的输出目录下，应该有三个输出文件，对应这三个任务
        job.setNumReduceTasks(3);

        //true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        job.waitForCompletion(true);

        return job.isSuccessful()?0:1;
    }
}
