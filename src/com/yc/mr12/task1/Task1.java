package com.yc.mr12.task1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.Map.Entry;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Task1 extends Configured implements Tool {

    /**
     * 计数器
     * @param
     * @return
     * @throws Exception
     */
    enum Counter{
        TiMESKIP,       //时间格式有误
        OUTOFTIMESKIP,  //时间不在参数指定的时间段内
        LINESKIP,       //源文件行有误
        USERSKIP        //某个用户某个时间段被整个抛弃
    }



//tl.set("0000000000\t0054775807\t00000179\t2021-07-01 00:08:46\twww.jd.com",false,"2021-07-01",new String[]{"07","17","24"});
    public static void main(String[] args) throws Exception {
        //参数格式要求
        if (args.length != 4) {
            System.err.println("");
            System.err.println("Usage: BaseStationDataPreprocess < input path > < output path > < date > < timepoint >");
            System.err.println("Example: BaseStationDataPreprocess /user/james/Base /user/james/Output 2012-09-12 07-09-17-24");
            System.err.println("Warning: Timepoints should be begined with a 0+ two digit number and the last timepoint should be 24");
            System.err.println("Counter:");
            System.err.println("\t" + "TIMESKIP" + "\t" + "Lines which contain wrong date format");
            System.err.println("\t" + "OUTOFTIMESKIP" + "\t" + "Lines which contain times that out of range");
            System.err.println("\t" + "LINESKIP" + "\t" + "Lines which are invalid");
            System.err.println("\t" + "USERSKIP" + "\t" + "Users in some time are invalid");
            System.exit(-1);
        }
        //运行任务
        int res= ToolRunner.run(new Configuration(),new Task1(),args);
        System.exit(res);
    }


    //这些东西也可以写在main函数
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf=getConf();
        //读取命令行参数，存到Configuration中，这样Mapper和reducer就可以访问到了
        conf.set("date",args[2]);
        conf.set("timepoint",args[3]);

        Job job=new Job(conf,"1_计算出不同用户在不同时段在不同基站停留的时间");
        job.setJarByClass(Task1.class);

        //输入路径
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //输出路径
        Path outputPath=new Path(args[1]);
        FileOutputFormat.setOutputPath(job,outputPath);
        //创建输出文件，并判断这个输出文件是否存在
        FileSystem fs=FileSystem.get(conf);
        if (fs.exists(outputPath)){
            fs.delete(outputPath,true);
        }
        //调用上面Map类作为Map任务代码
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        return job.isSuccessful()?0:1;
    }

    public static class Map extends Mapper<LongWritable,Text, Text,Text>{
        String date;
        String[] timepoint;
        boolean datasource;

        /**
         * 初始化：setup()执行一次，，读取配置
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取配置，获取参数
            this.date=context.getConfiguration().get("date");
            this.timepoint=context.getConfiguration().get("timepoint").split("-");
            //提取输入的文件名，判断是POS.txt还是NET.txt
            //获取当前split的文件（根据文件块的大小）
            FileSplit fs= (FileSplit) context.getInputSplit();
            //获取文件名
            String fileName=fs.getPath().getName();
            if(fileName.startsWith("POS")){
                datasource=true;
            }else if(fileName.startsWith("NET")){
                datasource=false;
            }else {
                throw new IOException("File Name should starts with POS or NET");
            }

        }


        /**
         * MAP任务
         * 读取基站数据
         * 找出数据对应的时间段
         * 以IMSI和时间段作为KEY
         * CGI和时间作为VALUE
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            TableLine tableLine=new TableLine();

            //读取行
            try{
                tableLine.set(line,this.datasource,this.date,this.timepoint);
            }catch (LineException e){
                if(e.getFlag()==-1){
                    //mapreduce中的一个Map<xxx，数量>
                    //获取计数器
                    context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
                }else {
                    context.getCounter(Counter.TiMESKIP).increment(1);
                }
                return;
            }catch (Exception e){
                context.getCounter(Counter.LINESKIP).increment(1);
                return;
            }
            //   imsi   timeFlag  position  date
            //0000000000|00-07    00000179|1625069326
            context.write(tableLine.outKey(),tableLine.outValue());
        }


    }

    /**
     * 统计同一个IMSI在同一个时间段
     */
    public static class Reduce extends Reducer<Text,Text, NullWritable,Text>{
        private String date;
        private SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.date=context.getConfiguration().get("date");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //手机号
            String imsi=key.toString().split("\\|")[0];
            //时间段
            String timeFlag=key.toString().split("\\|")[1];

            //用一个TreeMap(排好序)记录时间，它可以按键排序，这样，只要吧时间戳放在键上就可以排序了
            //时间戳   基站
            TreeMap<Long,String> uploads=new TreeMap<>();
            String valueString;
            for(Text value:values){
                valueString=value.toString();
                try {
                    //因为需要按时间排序，把时间作为键，把基站作为值
                    uploads.put(Long.valueOf(valueString.split("\\|")[1]),valueString.split("\\|")[0]);
                }catch (NumberFormatException e){
                    //时间戳格式错误，无法进行Long.valueOf()计算
                    context.getCounter(Counter.TiMESKIP).increment(1);
                    continue;
                }
            }
            try {
                //在最后添加“OFF”位置，即本时间段中，最后一个时间点
                //temp的值：  2021-07-01 07:00:00
                Date tmp=this.formatter.parse(this.date+" "+timeFlag.split("-")[1]+":00:00");
                //获取时间戳
                uploads.put((tmp.getTime()/1000L),"OFF");
                //汇总数据    <基站，停留时长>
                HashMap<String,Float> locs=getStayTime(uploads);
                //输出
                for(Entry<String, Float> entry:locs.entrySet()){
                    StringBuilder builder=new StringBuilder();
                    builder.append(imsi).append("|");
                    builder.append(entry.getKey()).append("|");
                    builder.append(timeFlag).append("|");
                    builder.append(entry.getValue());

                    context.write(NullWritable.get(),new Text(builder.toString()));
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        private HashMap<String,Float> getStayTime(TreeMap<Long,String> uploads){
            Entry<Long,String> upload,nextUpload;
            HashMap<String,Float> locs=new HashMap<>();
            //初始化
            //获取迭代器
            Iterator<Entry<Long,String>> it=uploads.entrySet().iterator();
            //先取一个
            upload=it.next();
            //计算
            while(it.hasNext()){
                nextUpload=it.next();
                //用后一条的时间戳-前有一条的时间戳        /     60     得到分钟
                float diff=(nextUpload.getKey()-upload.getKey())/60.0f;
                if(diff<=60.0){
                    //判断一个Map是否已有某个键
                    if(locs.containsKey(upload.getValue())){
                        locs.put(upload.getValue(),locs.get(upload.getValue())+diff);
                    }else{
                        locs.put(upload.getValue(),diff);
                    }
                }
                upload=nextUpload; //将当前这条数据，当做前一条数据，再循环
            }
            return locs;
        }
    }

}
