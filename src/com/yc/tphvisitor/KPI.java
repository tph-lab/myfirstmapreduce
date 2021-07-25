package com.yc.tphvisitor;

import java.util.HashSet;
import java.util.Set;

//python、bot、spider
public class KPI {

    private String remote_addr; //记录客户端的ip地址
    private String remote_user; //记录客户端用户名称,忽略属性"-"
    private String time_local;  //记录访问时间与时区
    private String request;     //记录请求的url与http协议
    private String status;      //记录请求状态。成功是200
    private String body_bytes_sent;     //记录发送给客户端文件主体内容大小
    private String http_referer;        //用来记录从哪个页面链接访问过来的
    private String http_user_agent;     //记录客户浏览器的相关信息
    private boolean valid=true;     //判断数据是否合法

    public String getRemote_addr() {
        return remote_addr;
    }
    public String getRemote_user() {
        return remote_user;
    }
    public String getTime_local() {
        return time_local;
    }
    public String getRequest() {
        return request;
    }
    public String getStatus() {
        return status;
    }
    public String getBody_bytes_sent() {
        return body_bytes_sent;
    }
    public String getHttp_referer() {
        return http_referer;
    }
    public String getHttp_user_agent() {
        return http_user_agent;
    }
    public boolean isValid() {
        return valid;
    }
    public void setRemote_addr(String remote_addr) {
        this.remote_addr = remote_addr;
    }
    public void setRemote_user(String remote_user) {
        this.remote_user = remote_user;
    }
    public void setTime_local(String time_local) {
        this.time_local = time_local;
    }
    public void setRequest(String request) {
        this.request = request;
    }
    public void setStatus(String status) {
        this.status = status;
    }
    public void setBody_bytes_sent(String body_bytes_sent) {
        this.body_bytes_sent = body_bytes_sent;
    }
    public void setHttp_referer(String http_referer) {
        this.http_referer = http_referer;
    }
    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }
    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String toString(){
        StringBuilder sb=new StringBuilder();
        sb.append("\nvalid:"+this.valid);
        sb.append("\nremote_addr:"+this.remote_addr);
        sb.append("\ntime_local:"+this.time_local);
        sb.append("\nrequest:"+this.request);
        sb.append("\nstatus:"+this.status);
        sb.append("\nbody_bytes_sent:"+this.body_bytes_sent);
        sb.append("\nhttp_referer:"+this.http_referer);
        sb.append("\nhttp_user_agent:"+this.http_user_agent);
        return sb.toString();
    }

    //过滤记录
    public static KPI filterPVs(String line){
        KPI kpi=parser(line);
        //要统计的页面路径：这样只对网站中某些目录的请求进行统计，其他不计，在上线项目中，可以使用配置文件来完成
        Set<String> pages=new HashSet<>();
        pages.add("/");
        pages.add("/about");
        pages.add("/back-ip-list/");
        pages.add("/cssandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-mahout-roadmap/");

        if(!pages.contains(kpi.getRequest())){
            kpi.setValid(false);
        }
        return kpi;
    }

    /**
     * 链式调用，KPI.parser(line).filter1().filter2().filter3();
     * jquery中   $().xxx().xxx().ccss()
     * 职责链设计模式
     */
    public static KPI parser(String line){
        KPI kpi=new KPI();
        String[] arr=line.split(" ");
        if(arr.length>11){
            kpi.setRemote_addr(arr[0]);
            kpi.setRemote_user(arr[1]);
            kpi.setTime_local(arr[3].substring(1));
            kpi.setRequest(arr[6]);
            kpi.setStatus(arr[8]);
            kpi.setBody_bytes_sent(arr[9]);
            kpi.setHttp_referer(arr[10]);

            //以“做分隔符
            String[] ss=line.split("\"");
            kpi.setHttp_user_agent(ss[ss.length-1]);

            if(Integer.parseInt(kpi.getStatus())>=400){
                //400表示找不到，500表示服务器错误，300表示重定向（304重定向到其他页面，302重定向到缓存）
                kpi.setValid(false);
            }
        }else{
            kpi.setValid(false);
        }
        return kpi;
    }





    public static void main(String[] args) {
        String line="194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] \"GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1\" 304 0 \"-\" \"Mozilla/4.0 (compatible;)";
        //测试算法
        System.out.println(line);
        KPI kpi=new KPI();
        String[] arr=line.split(" ");
        kpi.setRemote_addr(arr[0]);
        kpi.setRemote_user(arr[1]);
        kpi.setTime_local(arr[3].substring(1));
        kpi.setRequest(arr[6]);
        kpi.setStatus(arr[8]);
        kpi.setBody_bytes_sent(arr[9]);
        kpi.setHttp_referer(arr[10]);
        String[] ss=line.split("\"");
        kpi.setHttp_user_agent(ss[ss.length-1]);
        System.out.println(kpi+"----------------------------");
        KPI kpi2=KPI.parser(line);
        System.out.println(kpi2);
    }

    //按照page的独立ip进行分类
    public static KPI filterIPs(String line){
        KPI kpi=parser(line);
        Set<String> pages=new HashSet<>();
        pages.add("/");
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");
        if(!pages.contains(kpi.getRequest())){
            kpi.setValid(false);
        }
        return kpi;
    }

    //PV按照浏览器分类
    public static KPI filterBroswer(String line){
        return filterPVs(line);
    }

    //PV按照小时进行分类
    public static KPI filterTime(String line, String date){
        System.out.println(line+"nnnnnnnnnnnnnnn");
        KPI kpi=parser(line);
        if(kpi.getTime_local()!=null){
            if(kpi.getTime_local().indexOf(date)==-1){
                kpi.setValid(false);
            }
        }

        return kpi;
    }

    //PV按照域名进行分类




}
