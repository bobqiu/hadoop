package com.qiu.mptest.dismultiple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

/**
 * 去重
 * Created by Administrator on 2016/7/1.
 * 参照：http://www.369bi.com/hadoop%E4%B9%8Bmapreduce%E5%AE%9E%E8%B7%B5%EF%BC%88-%EF%BC%89%E5%8E%BB%E9%87%8D/
 */
public class DisMultiple {
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line=value;
            context.write(line, new Text(""));
        }

    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        Job job = Job.getInstance(conf,"DisMultiple job");

        String input = "hdfs://DamHadoop1:9000/user/hadoop/";
        String output = "hdfs://DamHadoop1:9000/user/hadoop/output/result";


        job.setJobName("DisMultiple");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass( Reduce.class);
        job.setReducerClass(Reduce.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        job.waitForCompletion(true);
        System.exit(0);
    }
}
