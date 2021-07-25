package com.yc.mr10.p4;

import com.yc.mr10.p3.CityCountMap;
import com.yc.mr10.p3.CityCountReducer;
import com.yc.mr10.p3.CityMap2;
import com.yc.mr10.p3.CityReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class App {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        //判断输入路径  *********有三个路径要输出     输入的文件路径     第一个job的输出目录     第二个job的输出目录
        if(args==null || args.length!=2){
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
        Job job=Job.getInstance(conf,"男女比例...");
        job.setJarByClass(App.class);

        //设置Reducer阶段的处理类
        job.setMapperClass(CountMap.class);//必须指定类型
        job.setReducerClass(CountReducer.class);
        job.setCombinerClass(CountCombiner.class);

        FileInputFormat.addInputPath(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        System.exit(job.waitForCompletion(true)?0:1);

    }

}
