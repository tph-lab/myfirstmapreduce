package com.yc.mr9.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Weather implements WritableComparable<Weather> {
    private int year;
    private int month;
    private int day;

    private int degree;

    @Override
    public void readFields(DataInput in) throws IOException {
        //读的顺序要按照写的顺序来
        this.year= in.readInt();
        this.month=in.readInt();
        this.day=in.readInt();
        this.degree=in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeInt(degree);
    }

    //没有被执行
    @Override
    public int compareTo(Weather o) {
        System.out.println("执行bean方法里的compareTo。。。。。。。。。。");
        //先比较年
        int t1=Integer.compare(this.year,o.getYear());
        if(t1==0){
            //再比较月
            int t2=Integer.compare(this.month,o.getMonth());
            if(t2==0){
                //最后比较温度
                return Integer.compare(this.degree,o.getDegree());
            }
            return t2;
        }
        return t1;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public int getDegree() {
        return degree;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public void setDegree(int degree) {
        this.degree = degree;
    }

    @Override
    public String toString() {
        return "Weather{" +
                "year=" + year +
                ", month=" + month +
                ", day=" + day +
                ", degree=" + degree +
                '}';
    }
}
