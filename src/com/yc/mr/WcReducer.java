package com.yc.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


    @Override           //输入键       因为经过了shuffle，进行了合并，所以一定是迭代
    protected void reduce(Text text, Iterable<IntWritable> iterable,Context context) throws IOException, InterruptedException {
        /**
         * reduce任务:  它的键 :AM, 它的值:org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@783841e2
         * reduce任务:  它的键 :HELLO, 它的值:org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@783841e2
         * reduce任务:  它的键 :I, 它的值:org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@783841e2
         * reduce任务:  它的键 :THE, 它的值:org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@783841e2
         * reduce任务:  它的键 :TPH, 它的值:org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@783841e2
         * reduce任务:  它的键 :WORLD, 它的值:org.apache.hadoop.mapreduce.task.ReduceContextImpl$ValueIterable@783841e2
         */
        System.out.println("reduce任务:  它的键 :"+ text+", 它的值:"+ iterable.toString());
        int sum=0;
        //for强循环
        for( IntWritable i:iterable){
            sum+=i.get();
        }
        context.write(text, new IntWritable(sum));
    }
//    AM	1
//    HELLO	1
//    I	1
//    THE	2
//    TPH	1
//    WORLD	1

}