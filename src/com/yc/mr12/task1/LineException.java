package com.yc.mr12.task1;

public class LineException extends Exception{

   int flag;
   public LineException(String msg,int flag){
       super(msg);
       this.flag=flag;
   }

   public int getFlag(){
       return flag;
   }
}
