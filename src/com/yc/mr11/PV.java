package com.yc.mr11;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

//按照PV数
public class PV {

    public static class KPIPVMapper extends Mapper<Object, Text,Text, IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable one=new IntWritable(1);
            Text word=new Text();
            //方案一：调用了filterPVs方法，对部分路径进行计数
            KPI kpi=KPI.filterIPs(value.toString());
            //另外  filterPVs()结果也是KPI,还可以接续调用filterxxx()来进行下一个过滤，职责链的设计模式
            //方案二：对所有路径进行计数
            //KPI kpi=KPI.parser(value.toString());
            if(kpi.isValid()){
                word.set(kpi.getRequest());
                context.write(word,one);
            }

        }

    }


    public static class KPIPVReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:iterable){
                sum+=i.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf=new Configuration();
        Path inputPath=new Path(args[0]);
        Path outputPath=new Path(args[1]);
        //创建输出文件，并判断这个输出文件是否存在
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
        Job job=new Job(conf,"PV统计");

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setJarByClass(PV.class);

        job.setMapperClass(KPIPVMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /**
         * 设置reducer的个数，有几个reducer，就有几个输出文件
         * 这里只能设置为1，因为如果分为多个文件的话，每个文件中的结果是有序的，但不保证全局有序
         */
        job.setNumReduceTasks(1);

        job.setReducerClass(KPIPVReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }

}


