package com.yc.mr10.p1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Servlet:   init-->service-->doGet-->doPost-->distroy
 * Reduce: setup-->reduce-->cleanup       使用run进行组装
 */
public class CountCombiner extends Reducer<Text, LongWritable,Text,LongWritable> {

    private LongWritable result=new LongWritable();

    public CountCombiner(){
        System.out.println("CountCombiner的构造方法");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("CountCombiner的setup方法");
    }

    //输入：相同键的数据，已经进行了合并(即一个键对应可能多个值（一个集合）在一个map中)
    //输出：进行了处理
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("键aaaaaaaaaa："+key.toString());
        System.out.println("CountCombiner的reduce方法");
        Long sum=new Long(0);
        for(LongWritable val:values){
            System.out.println("值aaaaaaaaaaaaaaa："+val.get());
            sum+=val.get();
        }
        result.set(sum);
        context.write(key,result);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("CountCombiner的cleanup方法");
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.out.println("CountCombiner的run方法");
        super.run(context);
    }
}
