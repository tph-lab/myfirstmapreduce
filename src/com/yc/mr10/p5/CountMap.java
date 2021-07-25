package com.yc.mr10.p5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMap extends Mapper<LongWritable, Text,Text,LongWritable> {

    @Override//统计不同所有权(10列)，型号(6列)和类型(9列)汽车的销售数量及比例
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs=value.toString().trim().split(",");
        if(strs!=null && strs.length==39 && strs[5]!=null &&  strs[9]!=null){
            context.write(new Text(strs[5]+"\t"+strs[9]),new LongWritable(1));
        }
    }
}
