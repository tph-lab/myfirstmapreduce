package com.yc.mr10.p4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CountReducer extends Reducer<Text, LongWritable,Text,DoubleWritable> {

    Map<String,Long> map=new HashMap<>();
    double all=0;//车辆总和

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum=0;
        for(LongWritable val:values){
            sum+=val.get();
        }
        all+=sum;
        map.put(key.toString(),sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<String> keySet=map.keySet();
        for(String key:keySet){
            long value=map.get(key);
            System.out.println("all:"+all);
            System.out.println("value:"+value);
            double percent=value/all;
            context.write(new Text(key),new DoubleWritable(percent));
        }
    }
}
