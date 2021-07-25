package com.yc.mr8;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//这个案例演示在本地执行，完全不依赖于hdfs和mapreduce集群.
public class App{
    public static void main(String[] args) throws Exception {
        if(null==args || args.length!=2){

            System.exit(1);
        }
        Path inputPath=new Path(args[0]);
        Path outputPath=new Path(args[1]);

        Configuration conf=new Configuration();
        Job job=new Job(conf,"在搜狗数据集中统计每个UID搜索次数");
        job.setJarByClass(App.class);

        //多加了一句combiner的指定
        job.setCombinerClass(Reducer.class);//默认Reducer类

        //TODO:有是那个计算节点，如果不设置，则partitioner不起作用
        job.setNumReduceTasks(2);
        //TODO:指定分区器
        job.setPartitionerClass(WordCountPartitioner.class);

        job.setMapperClass(UidMapper.class);
        job.setReducerClass(UidReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,inputPath);
        //创建输出文件，并判断这个输出文件是否存在
        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
        FileOutputFormat.setOutputPath(job,outputPath);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
