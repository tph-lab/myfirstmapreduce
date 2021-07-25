package com.yc.mr10.p1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMap extends Mapper<LongWritable, Text,Text,LongWritable> {

    public CountMap(){
        System.out.println("CountMap的构造方法");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("CountMap的setup方法");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("CountMap的cleanup方法");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("CountMap的map方法");
        String[] strs=value.toString().trim().split(",");
        if(strs!=null && strs.length>10 && strs[10]!=null){
            String type=strs[10];
            if("非营运".equals(type)){
                context.write(new Text("乘用车辆"),new LongWritable(1));
            }else {
                context.write(new Text("商用车辆"),new LongWritable(1));
            }
        }
    }



    @Override
    public void run(Context context) throws IOException, InterruptedException {
        System.out.println("CountMapd的run方法，下面的super，run一定要调用");
        //在super.run中控制countmap的生命周期方法的调用顺序
        super.run(context);
    }



}
