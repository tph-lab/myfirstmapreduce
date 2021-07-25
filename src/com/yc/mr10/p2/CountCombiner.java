package com.yc.mr10.p2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//也是继承reduce类
//输入：相同键的数据，已经进行了合并(即一个键对应可能多个值（一个集合）在一个map中)
//输出：进行了处理
public class CountCombiner extends Reducer<IntWritable,LongWritable,IntWritable,LongWritable> {

    //定义各个用户搜索次数的变量
    private LongWritable result=new LongWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum=new Long(0);
        for (LongWritable val:values){
            sum+=val.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
