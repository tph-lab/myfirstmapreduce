package com.yc.mr9;

import com.yc.mr9.bean.Weather;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WeatherGroup extends WritableComparator {
    public WeatherGroup(){
        super(Weather.class,true);
        System.out.println("WeatherGroup构造方法");
    }

    //根据比较进行分组
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        System.out.println("WeatherGroup的比较：只需要比较年月，而不需要比较温度");
        Weather w1= (Weather) a;
        Weather w2= (Weather) b;
        int c1=Integer.compare(w1.getYear(),w2.getYear());
        System.out.println(w1.toString()+"group分组"+w2.toString());
        if(c1==0){
            int c2=Integer.compare(w1.getMonth(),w2.getMonth());
            return c2;
        }
        return c1;
    }
}
