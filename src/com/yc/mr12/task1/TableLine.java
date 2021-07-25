package com.yc.mr12.task1;

import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 读取一行数据
 * 提取所要字段
 */
public class TableLine {

    //连续定义
    private String imsi,position,time,timeFlag;
    private Date day;
    private SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    /**
     *
     * @param line   原始的字段
     * @param source  文件类型POS.txt还是NET.txt，他们的数据列（时间）不同
     * @param date   要处理数据的日期
     * @param timepoint  解析处理的时间段
     * @throws LineException
     */
    public void set(String line,boolean source,String date,String []timepoint) throws LineException {
        String[] lineSplit=line.split("\t");
        //判断是POS(source=true)还是NET(source=false)文件，它们的位置不一样
        if(source){
            this.imsi=lineSplit[0];
            this.position=lineSplit[3];
            this.time=lineSplit[4];
        }else {
            this.imsi=lineSplit[0];
            this.position=lineSplit[2];
            this.time=lineSplit[3];
        }
        //检查日期合法性
        if(!this.time.startsWith(date)){        //年月日必须与date一致
            throw new LineException("",-1);
        }
        try {
            /**
             * 1.parse():将String的对象根据 模板提供的yyyy-mm-dd进行转化成为Date类型，如果String的对象不是指定的模板类型的话就会报错
             * 2.format():将Date对象根据模板转化为String对象。
             */
            this.day=this.formatter.parse(this.time);
        }catch (ParseException e){
            throw new LineException("",0);
        }
        //*****计算所属时间段
        int i=0;
        //有几个时段：timepoint: 07-17-24
        int n=timepoint.length;
        //this.time:    2021-07-01 10:58:08    取出小时部分
        int hour=Integer.valueOf(this.time.split(" ")[1].split(":")[0]);
        while (i<n && Integer.valueOf(timepoint[i])<=hour){
            i++;
        }
        if(i<n){
            if(i==0){
                this.timeFlag=("00-"+timepoint[i]);
            }else{
                this.timeFlag=(timepoint[i-1]+"-"+timepoint[i]);
            }
        }else{
            throw new LineException("",-1);
        }
    }

    /**
     * 输出key
     */
    public Text outKey(){
        return new Text(this.imsi+"|"+this.timeFlag);
    }

    /**
     * 输出VALUE
     */
    public Text outValue(){
        //day.getTime()    时间戳    1970.1.1  以来的毫秒数
        long t=(this.day.getTime()/1000L);       //用时间的偏移量作为输出时间
        return new Text(this.position+"|"+String.valueOf(t));
    }

    public static void main(String[] args) throws LineException {
        TableLine tl=new TableLine();
        tl.set("0000000000\t0054775807\t00000179\t2021-07-01 00:08:46\twww.jd.com",false,"2021-07-01",new String[]{"07","17","24"});
        System.out.println(tl.outKey().toString()+"    "+tl.outValue().toString());
        //   imsi   timeFlag  position  date
        //0000000000|00-07    00000179|1625069326
    }
}
