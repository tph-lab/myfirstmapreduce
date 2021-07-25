package com.yc.mr10.p2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class CountReducer extends Reducer<IntWritable, LongWritable,IntWritable, Text> {

    //java.util.map键        是set，无序不可重复
    private Map<Integer,Long> map=new HashMap<>();
    //总销售数量
    private Long total=0L;
    //比例
    private DoubleWritable num=new DoubleWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //1.sum
        Long sum=0L;
        for(LongWritable v:values){
            sum+=v.get();
        }
        //2.total:global num
        total+=sum;
        //3.put to hashmap
        map.put(key.get(),sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //思考：为什么要用TreeSet           TreeSet是有序
        //map.keySet()-->Set(无序不重复)-->TreeSet-->有序不重复
        TreeSet<Integer> keys=new TreeSet<>(map.keySet());
        //Set<Integer> keys=map.keySet();
        for(Integer key:keys){
            Long value=map.get(key);
            double percent=value/(double) total;
            num.set(percent);
            context.write(new IntWritable(key),new Text(value+"\t"+num));
        }
    }


}
