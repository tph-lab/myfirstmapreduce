package com.yc.mr10.p1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CountReducer extends Reducer<Text, LongWritable,Text,Text> {

    //因为要在reduce（）中全局累计，就可以在cleanup中读到这个全局变量，然后就可以计算比例
    private Map<String,Long> map=new HashMap<>();

    //车辆总和（两个类别）
    private double all=0;

    public CountReducer(){
        System.out.println("CountReducer的构造方法");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("CountReducer的setup方法");
    }

    //输入的是已经经过CountCominer合并的类
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        System.out.println("CountReduce类中的reduce方法的调用");
        System.out.println("键zzzzzzzzzzzzzzzz："+key.toString());
        //这个类key对应的累加和
        long sum=0;
        for(LongWritable val:values){
            System.out.println("值zzzzzzzzzzz："+val.get());
            sum+=val.get();
        }
        all+=sum;
        map.put(key.toString(),sum);
        //没有输出：因为是要计算各类型的车所占的比例，必须等到所有的reduce结束后才能算，所以这里不能输出，只能将输出写到cleanup()中
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("CountReduce中的cleanup方法的调用");
        Set<String> keySet=map.keySet();    //得到键集
        for(String key:keySet){
            long value=map.get(key);
            //求乘用车辆和商用车辆的比例
            double percent=value/all;
            //假设：输出的部分包括：类别名    数量比\t百分比
            context.write(new Text(key),new Text(value+"\t"+percent));
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.out.println("CountReducer的run方法，注意下面的super.run()要调用");
        //父类中的决定reduce的生命周的方法调用顺序
        super.run(context);
    }
}
