package com.yc.mr8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UidReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result=new IntWritable();   //定义各个用户搜索次数的变量

    public UidReducer() {
        System.out.println("UidReducer构造方法");
    }

    /**
     * 对map处理后的中间结果做最后处理
     * key:map处理完输出的中间结果键值对的键
     * values:map函数处理完后输出的中间结果键值对的列列表
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override           //输入键       因为经过了shuffle，进行了合并，所以一定是迭代
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum=0;
        //for强循环
        for( IntWritable val:values){
            sum+=val.get();
        }
        result.set(sum);
        context.write(key, result);

    }


}