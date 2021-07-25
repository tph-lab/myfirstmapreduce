package com.yc.mr8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class WordCountPartitioner extends Partitioner<Text, IntWritable> {

    public WordCountPartitioner(){
        System.out.println("WordCountPartitioner构造方法");
    }

    @Override
    public int getPartition(Text key, IntWritable intWritable, int numPartitions) {
        System.out.println("注意分区数："+numPartitions);
        int a=key.hashCode()%numPartitions;     //numPartitions由reducer数目决定
        System.out.println("分区a的值："+a);
        if(a>=0){
            return a;
        }
        return 0;
    }
}
