package com.yc.mr11.util;



public class IPtest {

    public static void main(String[] args) {
        //指定纯真数据库的文件名，所在文件夹
        IPSeeker ip= IPSeeker.getInstance();
        //测试IP 58.20.43.13
        System.out.println(ip.getAddress("58.20.43.13"));
    }

}