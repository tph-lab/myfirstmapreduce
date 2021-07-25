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
import java.util.HashSet;
import java.util.Set;

public class IP {

    public static class KPIIPMapper extends Mapper<Object, Text,Text, Text> {
        private Text word=new Text();
        private Text ips=new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            KPI kpi=KPI.filterPVs(value.toString());
            if(kpi.isValid()){
                word.set(kpi.getRequest());//请求的资源地址
                ips.set(kpi.getRemote_addr());//客户端IP
                context.write(word,ips);
            }
        }
    }

    public static class KPIIPReducer extends Reducer<Text,Text,Text,Text> {
        private Text result=new Text();
        //使用set完成去重
        private Set<String> count=new HashSet<>();

        @Override
        protected void reduce(Text key, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {
            for(Text i:iterable){
                count.add(i.toString());
            }
            //IP数
            result.set(String.valueOf(count.size()));
            context.write(key,result);
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
        Job job=new Job(conf,"统计每个页面有多少个独立IP访问过");

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setJarByClass(IP.class);

        job.setMapperClass(KPIIPMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /**
         * 设置reducer的个数，有几个reducer，就有几个输出文件
         * 这里只能设置为1，因为如果分为多个文件的话，每个文件中的结果是有序的，但不保证全局有序
         */
        job.setNumReduceTasks(1);

        job.setReducerClass(KPIIPReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }




}
