package com.yc.mr9;

import com.yc.mr9.bean.Weather;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeatherReducer extends Reducer<Weather, IntWritable, Text, NullWritable> {

    public WeatherReducer(){
        System.out.println("WeatherReducer构造");
    }


    @Override
    public void reduce(Weather weather, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {
        System.out.println("执行reduce方法.......");
        System.out.println("reduce的weather："+weather.toString());
        int count=0;
        for(IntWritable value:iterable){
            System.out.println("reduce每一条:"+value);
            count++;
            if(count>2){
                break;
            }
            String result=weather.getYear()+"-"+weather.getMonth()+"-"+weather.getDay()+"-"+value.get();
            context.write(new Text(result),NullWritable.get());//空字符串
        }

    }
}
