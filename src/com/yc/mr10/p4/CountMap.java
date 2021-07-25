package com.yc.mr10.p4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMap extends Mapper<LongWritable, Text,Text,LongWritable> {



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strs=value.toString().trim().split(",");
        if(strs!=null && strs.length==39 && strs[38]!=null){
            if("男性".equals(strs[38]) || "女性".equals(strs[38])){
                context.write(new Text(strs[38]),new LongWritable(1));
            }
        }
    }
}
