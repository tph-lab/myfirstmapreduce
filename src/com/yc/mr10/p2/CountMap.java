package com.yc.mr10.p2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMap extends Mapper<LongWritable, Text, IntWritable,LongWritable> {

    private IntWritable month=new IntWritable();
    private LongWritable num=new LongWritable();


    /**
     * 映射出每个月的销售数量，注意：数据第二列为月份，第12列为销售数量
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.split line
        String line=value.toString();
        String[] arr=line.split(",");
        if(arr.length>11 && null!=arr[11] && !"".equals(arr[11].trim())){
            try {
                month.set(Integer.parseInt(arr[1]));
                num.set(Long.parseLong(arr[11]));
                context.write(month,num);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
