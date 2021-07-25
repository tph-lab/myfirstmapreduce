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
import java.text.DecimalFormat;
import java.util.*;

public class TPH_Browser_Ratio {

    public static class KPIBrRAMapper extends Mapper<Object, Text,Text, IntWritable> {
        private Text word=new Text();
        private Text browers=new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            KPI kpi=KPI.filterBroswer(value.toString());
            if(kpi.isValid()){
                browers.set(kpi.getHttp_user_agent());//浏览器
                IntWritable num=new IntWritable(1);
                context.write(browers,num);
            }
        }
    }

    public static class KPIBrRAReducer extends Reducer<Text,IntWritable,Text,Text> {
        private Text resultKey=new Text();
        private Text resultValue=new Text();
        //使用set完成去重
        private Set<String> count=new HashSet<>();
        int total=0;
        private Map<String, Integer> map=new HashMap<>();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {
            int single=0;
            for(IntWritable i:iterable){
                single++;
                total++;
            }
            map.put(key.toString(),single);
        }



        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Set<String> keys=map.keySet();
            //Set<Integer> keys=map.keySet();
            for(String key:keys){
                Integer value=map.get(key);
                DecimalFormat df = new DecimalFormat("0.0000");//格式化小数
                String num = df.format((float)value/total);//返回的是String类型
                resultKey.set(key);
                resultValue.set(num);
                context.write(resultKey,resultValue);
            }
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
        Job job=new Job(conf,"统计浏览器");

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setJarByClass(TPH_Browser_Ratio.class);

        job.setMapperClass(KPIBrRAMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /**
         * 设置reducer的个数，有几个reducer，就有几个输出文件
         * 这里只能设置为1，因为如果分为多个文件的话，每个文件中的结果是有序的，但不保证全局有序
         */
        job.setNumReduceTasks(1);

        job.setReducerClass(KPIBrRAReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
