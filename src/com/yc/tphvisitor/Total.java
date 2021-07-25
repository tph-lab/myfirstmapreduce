package com.yc.tphvisitor;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Total {

    public static class KPISingleMapper extends Mapper<Object, Text,Text, Text> {
        private Text word=new Text();
        private Text ips=new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valuestr=value.toString();
            String []splits=valuestr.split(" ");
            if(!splits[3].startsWith("[19/Sep/2013")){
                KPI kpi0=KPI.parser(valuestr);
                if(kpi0.isValid()) {
                    word.set("yes                                                                                                                                                                                                                                         ");
                    ips.set(kpi0.getRemote_addr());
                    context.write(word, ips);
                }
            }else{
                //19号的
                KPI kpi=KPI.filterTime(value.toString(),"19/Sep/2013");
                if(kpi.isValid()){
                    word.set(kpi.getRequest());//请求的资源地址
                    ips.set(kpi.getRemote_addr());//客户端IP
                    context.write(word,ips);
                }
            }
        }
    }

    //map端,排序
    public  static class Sort extends WritableComparator {

        public Sort(){
            //分析WritableComparator源码
            super(Text.class,true);//一定要写，因为是根据键进行排序的，默认无参构造
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b){
            Text w1= (Text) a;
            Text w2= (Text) b;
            int c1=-Integer.compare(w1.toString().length(),w2.toString().length());
            return c1;
        }
    }


    public static class KPISingleReducer extends Reducer<Text,Text,Text,Text> {
        private Text result=new Text();
        //以前的用户
        String aes=new String();

        @Override
        protected void reduce(Text key, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {
            if(String.valueOf(key).equals("yes                                                                                                                                                                                                                                         ")){
                for (Text text:iterable){
                    aes+=text.toString();
                }
            }else{
                //使用set完成去重    访客数
                Set<String> count=new HashSet<>();

                //使用set完成去重    浏览量
                List<String> count2=new ArrayList<>();
                Set<String> newUser=new HashSet<>();
                for(Text i:iterable){
                    if(aes.indexOf(i.toString())==-1){
                        newUser.add(i.toString());
                    }
                    count.add(i.toString());
                    count2.add(i.toString());
                }
                DecimalFormat df = new DecimalFormat("0.00");//格式化小数
                String num = df.format((float)newUser.size()/count.size());//返回的是String类型

                //
                result.set("\t访客数:"+String.valueOf(count.size())+"\t浏览数:"+String.valueOf(count2.size())+"\t新访客数:"+String.valueOf(newUser.size())+"\t新访客比率:"+num);
                context.write(key,result);
            }

        }
    }

    public static class KPITotalMapper extends Mapper<Object, Text,Text, Text> {
        private Text word=new Text();
        private Text ips=new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            word.set("total");
            context.write(word,value);
        }
    }



    public static class KPITotalReducer extends Reducer<Text,Text,Text,Text> {
        private Text result=new Text();
        //   /blog	     访客数：1        浏览数：1          新访客数：1                  新访客比率：1.00
        int visitor=0;
        int view=0;
        int newVistor=0;
        @Override
        protected void reduce(Text key, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {
            for (Text text:iterable){
                String str=text.toString();
                String[] splits=str.split("\t");

                String visitor0=splits[2].substring(splits[2].indexOf("访客数:")+4);
                visitor+=Integer.parseInt(visitor0);
                view+=Integer.parseInt(splits[3].substring(splits[3].indexOf("浏览数:")+4));
                newVistor+=Integer.parseInt(splits[4].substring(splits[4].indexOf("新访客数:")+5));
            }
            DecimalFormat df = new DecimalFormat("0.00");//格式化小数
            String num = df.format((float)newVistor/visitor);//返回的是String类型
            result.set("\t访客数："+visitor+"\t浏览数："+view+"\t新访客数："+newVistor+"\t新访客比率："+num);
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
        Job job=new Job(conf,"每个网页");

        FileInputFormat.setInputPaths(job,inputPath);
        FileOutputFormat.setOutputPath(job,outputPath);

        job.setJarByClass(Total.class);

        job.setMapperClass(KPISingleMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(Sort.class);//指定排序类
        /**
         * 设置reducer的个数，有几个reducer，就有几个输出文件
         * 这里只能设置为1，因为如果分为多个文件的话，每个文件中的结果是有序的，但不保证全局有序
         */
        job.setNumReduceTasks(1);

        job.setReducerClass(KPISingleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);



        //创建job     job任务
        Job job2=Job.getInstance(conf,"整个网站");
        job2.setJarByClass(Total.class);

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
        job2.setMapOutputValueClass(Text.class);


        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        //设置Reducer阶段的处理类
        job2.setMapperClass(KPITotalMapper.class);//必须指定类型
        job2.setReducerClass(KPITotalReducer.class);

        //true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束，如果不写则不显示
        job2.waitForCompletion(true);

    }

}
