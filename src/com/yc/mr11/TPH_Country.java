package com.yc.mr11;

import com.yc.mr11.util.IPSeeker;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class TPH_Country {

    public static class KPICounMapper extends Mapper<Object, Text,Text, IntWritable> {
        private Text word=new Text();

        //以IP转化后的国家为键，以1为值
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            KPI kpi=KPI.filterBroswer(value.toString());
            if(kpi.isValid()){
                //指定纯真数据库的文件名，所在文件夹
                IPSeeker ip= IPSeeker.getInstance();
                String addr=ip.getAddress(value.toString().split("\t")[0]);
                String str=addr.split(" ")[0];
                word.set(str);
                context.write(word,new IntWritable(1));
            }
        }
    }

    public static class KPICounReducer extends Reducer<Text,IntWritable,Text,Text> {
        private int total;
        private Map<String,Integer> map=new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {
            int num=0;
            for(IntWritable i:iterable){
                num++;
                total++;
            }
           map.put(key.toString(),num);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<Entry<String, Integer>> entrySet=map.entrySet();
            for (Entry entry:entrySet){
                int num= (int) entry.getValue();
                String str= (String) entry.getKey();
                DecimalFormat df = new DecimalFormat("0.0000");//格式化小数
                String numstr = df.format((float)num/total);//返回的是String类型
                context.write(new Text(str),new Text(numstr));

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

        job.setJarByClass(TPH_Country.class);

        job.setMapperClass(KPICounMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /**
         * 设置reducer的个数，有几个reducer，就有几个输出文件
         * 这里只能设置为1，因为如果分为多个文件的话，每个文件中的结果是有序的，但不保证全局有序
         */
        job.setNumReduceTasks(1);

        job.setReducerClass(KPICounReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }

}
