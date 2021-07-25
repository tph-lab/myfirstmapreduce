package com.yc.mr9;

import com.yc.mr9.bean.Weather;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

//map端分区
public class WeatherPartition extends Partitioner<Weather, IntWritable> {

    public WeatherPartition(){
        System.out.println("WeatherPartition构造");
    }


    @Override           //      输入键                   值
    public int getPartition(Weather key, IntWritable intWritable, int numReduceTasks) {
        System.out.println("reduce数目为："+numReduceTasks);
        //写一个算法，计算hash
        //注意：1.这个hash算法要满足业务要求
        //2.每一对键值都会调用这个方法，所以这个里面的算法要简洁
        int result=(key.getYear()-1949)%numReduceTasks;
        System.out.println(key.toString()+"被分配在了："+result+"");
        return result;
    }
}
