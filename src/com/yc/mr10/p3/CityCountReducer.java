package com.yc.mr10.p3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CityCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    //统计4月的各市区县的销售数量，存为键值对

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long count=0L;
        for(LongWritable val:values){
            count+=val.get();
        }
        context.write(key,new LongWritable(count));     //市、区县
    }
}
