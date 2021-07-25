package com.yc.mr12.task2;

import com.yc.mr12.task1.LineException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Three extends Configured implements Tool {
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
        int res= ToolRunner.run(new Configuration(),new Three(),args);
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
        job.setJarByClass(Three.class);

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
        job.setMapperClass(Three.Map.class);
        job.setReducerClass(Three.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        return job.isSuccessful()?0:1;
    }

    public static class Map extends Mapper<LongWritable,Text, Text,Text> {
        String date;
        String[] timepoint;
        boolean datasource;



        /**
         * 初始化：setup()执行一次，，读取配置
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



        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();

            /**
             *  * 以手机号为键
             *  * sin卡号为值
             */
            context.write(new Text(line.split("\t")[1]),new Text(line.split("\t")[0]));
        }
    }


    public static class Reduce extends Reducer<Text,Text, Text,Text> {
        //使用set完成去重
        private Set<String> count=new HashSet<>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String str=new String();
            for(Text i:values){
                count.add(i.toString());
                str+=i.toString()+"\t";
            }
            if(count.size()>1){
                context.write(key,new Text(str));
            }

        }
    }
}
