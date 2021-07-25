package com.yc.mr9;


import com.yc.mr9.bean.Weather;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//map端,排序
public class WeatherSort extends WritableComparator {

    public WeatherSort(){
        //分析WritableComparator源码
        super(Weather.class,true);//一定要写，因为是根据键进行排序的，默认无参构造
        System.out.println("WeatherSort构造方法");
    }

    @Override
    public int compare(WritableComparable a,WritableComparable b){
        System.out.println("调用了    weathersort中的compare");
        Weather w1= (Weather) a;
        Weather w2= (Weather) b;
        System.out.println(w1.toString()+"与"+w2.toString()+"进行compare");
        int c1=Integer.compare(w1.getYear(),w2.getYear());
        if(c1==0){
            int c2=Integer.compare(w1.getMonth(),w2.getMonth());
            if(c2==0){
                //温度采用降序，所以计算后取反，而上面的年月是升序排序
                //默认升序排列，如需降序，则加个-
                return -Integer.compare(w1.getDegree(),w2.getDegree());
            }
            return c2;
        }
        return c1;
    }
}
