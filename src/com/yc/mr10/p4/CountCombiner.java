package com.yc.mr10.p4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountCombiner extends Reducer<Text, LongWritable,Text,LongWritable> {

    private LongWritable result=new LongWritable();

    public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {

        Long sum=new Long(0);
        for(LongWritable val:values){
            sum+=val.get();
        }
        result.set(sum);
        context.write(key,result);
    }

}
