package com.yc.mr10.p3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CityCountMap extends Mapper<LongWritable, Text,Text,LongWritable> {

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
       String[] str=value.toString().trim().split(",");
     //  str[1] 判断是否为4月份
        if(str!=null && str.length==39 && str[1].equals("4")){
            //str[2]市，str[3]区县
            String cityAndCountry=str[2]+","+str[3];
            context.write(new Text(cityAndCountry),new LongWritable(Long.parseLong(str[11])));
        }



    }
}
