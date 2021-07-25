package com.yc.mr10.p1;

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
        //判断输入路径
        if(args==null || args.length!=2){
            System.out.println("Please Input Full Path!");
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
        Job job=Job.getInstance(conf,"统计乘用车辆和商用车辆的数量和占比");
        job.setJarByClass(App.class);

        job.setMapperClass(CountMap.class);//必须指定类型
        job.setCombinerClass(CountCombiner.class);
        job.setReducerClass(CountReducer.class);

        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true)?0:1);
    }

}
