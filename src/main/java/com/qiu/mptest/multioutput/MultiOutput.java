package com.qiu.mptest.multioutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * Created by Administrator on 2016/7/6.
 */
public class MultiOutput {
    public static class MultiOutMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("MultiOutMapper: key:"+key+" value:"+value);
            String[] values = value.toString().split(";");
            context.write(new Text(values[0]), new Text(values[1]));
        }
    }

    public static class MutiOutReduce extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs mos;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
            }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String city = key.toString();
            for (Text value : values) {
                System.out.println("MutiOutReduce: key:"+key+" value:"+value);
                if (city.equals("bj")) {
                    mos.write("bj", key, value);
                } else if (city.equals("sh")) {
                    mos.write("sh", key, value);
                } else if (city.equals("sz")) {
                    mos.write("sz", key, value);
                }
            }
        }
    }
    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        Date startTime = new Date();
        System.out.println("job Start:" + startTime);
        String input1 = "hdfs://www.369bi.com:9000//user/hadoop/multiout/citycount";
      //  String input2 = "hdfs://www.369bi.com:9000//user/hadoop/mutisource/account";
        String output = "hdfs://www.369bi.com:9000/user/hadoop/multiout/result";
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        FileSystem fs=FileSystem.get(new URI(input1),conf);

        Path outPath = new Path(output);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        Job job = new Job(conf, "MultiOutput");

        job.setMapperClass(MultiOutMapper.class);
        FileInputFormat.addInputPath(job, new Path(input1));

        MultipleOutputs.addNamedOutput(job, "bj", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "sh", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "sz", TextOutputFormat.class, Text.class, Text.class);

        job.setJarByClass(MultiOutput.class);
        job.setReducerClass(MutiOutReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        int flag = job.waitForCompletion(true) ? 0 : 1;
        Date end_time = new Date();
        System.out.println("job end:" + end_time);
        System.out.println("The job takes:" + (end_time.getTime() - startTime.getTime()) + " ms.");
    }
}
