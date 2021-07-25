package com.yc.mr10.p3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class App {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //判断输入路径  *********有三个路径要输出     输入的文件路径     第一个job的输出目录     第二个job的输出目录
        if(args==null || args.length<3){
            System.err.println("Please Input Full Path!");
            System.exit(1);
        }
        Configuration conf=new Configuration();

        Path inputPath=new Path(args[0]);
        Path outputPath=new Path(args[1]);

        //创建输出文件，并判断这个输出文件是否存在
        FileSystem fs=FileSystem.get(conf);
        if(fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }

        //创建job     job任务
        Job job1=Job.getInstance(conf,"统计山西省2013年4月份各市区县的汽车销售的比例");
        job1.setJarByClass(App.class);


        FileInputFormat.addInputPath(job1,inputPath);
        FileOutputFormat.setOutputPath(job1,outputPath);


        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(LongWritable.class);


        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        //设置Reducer阶段的处理类
        job1.setMapperClass(CityCountMap.class);//必须指定类型
        job1.setReducerClass(CityCountReducer.class);

        job1.waitForCompletion(true);



        //创建job     job任务
        Job job2=Job.getInstance(conf,"统计山西省2013年4月份各市的汽车销售的比例");
        job2.setJarByClass(App.class);

        //将上一输出文件当成job2的输入文件
        Path inputPath2=new Path(args[1]);
        Path outputPath2=new Path(args[2]);

        //创建    输出文件，并判断这个输出文件是否存在
        if(fs.exists(outputPath2)){
            fs.delete(outputPath2,true);
        }


        FileInputFormat.addInputPath(job2,inputPath2);
        FileOutputFormat.setOutputPath(job2,outputPath2);


        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);


        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        //设置Reducer阶段的处理类
        job2.setMapperClass(CityMap2.class);//必须指定类型
        job2.setReducerClass(CityReducer2.class);

        //true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束，如果不写则不显示
        job2.waitForCompletion(true);


    }

}
