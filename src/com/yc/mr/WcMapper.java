package com.yc.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WcMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    @Override          //输入键           值
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        /*前面是偏移量  后面是值
         * map任务:输入的键为: 0 输入的值为:HELLO THE WORLD
         * map任务:输入的键为: 17 输入的值为:I AM THE TPH
         */
        System.out.println("map任务:输入的键为: "+ key.toString() +"输入的值为:"+ value.toString());
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while (stringTokenizer.hasMoreTokens()) {
           //打印当前分隔符和下一个分隔符之间的内容。
            word.set(stringTokenizer.nextToken());
                     //输出  键和值
            context.write(word, one);
        }
    }
}
