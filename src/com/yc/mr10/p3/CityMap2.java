package com.yc.mr10.p3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CityMap2 extends Mapper<LongWritable, Text,Text,LongWritable> {

    //从job1生成的文件中读取，所以key：仍然是偏移量      value:市、区县\t数量
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] str=value.toString().trim().split("\t");
        if(str!=null && str.length>1){
            //str[0]拆分出市和区县
            String[] city=str[0].split(",");
            if(city!=null && city.length>0){
                //市     数量
                context.write(new Text(city[0]),new LongWritable(Long.parseLong(str[1])));
            }
        }
    }
}
