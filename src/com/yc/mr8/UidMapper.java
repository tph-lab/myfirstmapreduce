package com.yc.mr8;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class UidMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);//用户搜索次数的单位
    private Text uidText = new Text();  //用户的id

    public UidMapper(){
        System.out.println("uidMapper构造方法");
    }


    /**
     * 完成切分操作：记录    uid:  1
     * key:代表读取500w条记录时，文本行的偏移量
     * value:某一行记录
     * context:代表mr框架的上下文
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override          //输入键           值
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line=value.toString();
        String[] arr=line.split("\t");
        if(null!=arr && arr.length==6){
            String uid=arr[1];      //取出用户id
            if(null!=uid && !"".equals(uid.trim())){
                uidText.set(uid);
                context.write(uidText,ONE);
            }

        }

    }
}
