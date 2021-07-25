package com.yc.mr9;

import com.yc.mr9.bean.Weather;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 注意：bean类的compareto不起作用
 *
 * 1、先map、sort（继承WritableComparator）、partition构造
 * 2、每一行数据执行map方法、分区、
 * 3、进行排序sort（按照分好的区进行排序，各个区排序同时进行）
 * 以下操作每个分区（按顺序）执行一次：
 * 4、sort、group（继承WritableComparator）、reducer构造
 * 5、每个分区进行边group分组（根据条件分组）、边reduce方法
 * 6、如果group分组条件返回0，则下次直接重新进入reduce
 */
public class WeatherMapper extends Mapper<LongWritable, Text, Weather, IntWritable> {
    public WeatherMapper() {
        System.out.println("WeatherMapper构造");
    }

    /**
     * 读取数据，切分成year,month,day,degree,并存放到weather对象中，再用context完成输出
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        System.out.println("执行map方法......");
        //1.调用   hadoop的工具方法完成，将value按"\t"切分
        String []strs=value.toString().split("\t");
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //单例静态
        Calendar cal=Calendar.getInstance();
        try {
            //将某字符串按照某格式转化为date
            Date d=sdf.parse(strs[0]);
            //设置时间
            cal.setTime(d);
            Weather w=new Weather();
            w.setYear(cal.get(Calendar.YEAR));
            w.setMonth(cal.get(Calendar.MONTH)+1);      //月从0开始算的
            w.setDay(cal.get(Calendar.DAY_OF_MONTH));

            int degree=Integer.parseInt(strs[1].substring(0,strs[1].lastIndexOf("c")));
            w.setDegree(degree);
            context.write(w,new IntWritable(degree));
        }catch (ParseException e){
            e.printStackTrace();
        }
    }
}
