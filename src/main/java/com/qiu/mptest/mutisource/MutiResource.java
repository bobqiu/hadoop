package com.qiu.mptest.mutisource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * Created by Administrator on 2016/7/5.
 */
public class MutiResource {
    public static class SalesRecordMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取当前split的文件名
           /* FileSplit fileSplit = (FileSplit) context.getInputSplit();
            System.out.println("fileSplit.getPath().getName():"+fileSplit.getPath().getName());
            System.out.println("fileSplit.getPath().toString():"+fileSplit.getPath().toString());
            System.out.println("fileSplit.getPath().getParent().toString():"+fileSplit.getPath().getParent().toString());
            System.out.println("fileSplit.getPath().getParent().getName():"+fileSplit.getPath().getParent().getName());
*/
            InputSplit split = context.getInputSplit();
            Class<? extends InputSplit> splitClass = split.getClass();

            FileSplit fileSplit = null;
            if (splitClass.equals(FileSplit.class)) {
                fileSplit = (FileSplit) split;
            } else if (splitClass.getName().equals(
                    "org.apache.hadoop.mapreduce.lib.input.TaggedInputSplit")) {
                // begin reflection hackery...

                try {
                    Method getInputSplitMethod = splitClass
                            .getDeclaredMethod("getInputSplit");
                    getInputSplitMethod.setAccessible(true);
                    fileSplit = (FileSplit) getInputSplitMethod.invoke(split);
                } catch (Exception e) {
                    // wrap and re-throw error
                    throw new IOException(e);
                }

                // end reflection hackery
            }
            System.out.println("###################fileSplit.getPath().getName():"+fileSplit.getPath().getName());
            System.out.println("############################fileSplit.getPath().toString():"+fileSplit.getPath().toString());
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("===================================SalesRecordMapper:"+key+":"+value.toString());
            String record = value.toString();
            String[] parts = record.split("\t");
            context.write(new Text(parts[0]),new Text("Sales\t"+parts[1]));
        }
    }

    public static class AccountRecordMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("***********************************AccountRecordMapper:"+key+":"+value.toString());
            String record = value.toString();
            String[] parts = record.split("\t");
            context.write(new Text(parts[0]), new Text("accout\t"+parts[1]));

        }
    }

    public static class MutiRecordReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("mutiRecordReduce:"+key.toString());
            String name = " ";
            double total = 0.0;
            int count = 0;
            for (Text value : values) {
              //  System.out.println("mutirecordreduce:"+key.toString()+" ::"+value.toString());
                String[] parts = value.toString().split("\t");
                if (parts[0].equals("Sales")) {
                    count++;
                    total += Float.parseFloat(parts[1]);
                } else if (parts[0].equals("accout")) {
                    name = parts[1];
                }
            }
            String str = String.format("%d\t%f", count, total);
            context.write(new Text(name), new Text(str));

        }
    }

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        Date startTime = new Date();
        System.out.println("job Start:" + startTime);
        String input1 = "hdfs://DamHadoop1:9000//user/hadoop/mutisource/sale";
        String input2 = "hdfs://DamHadoop1:9000//user/hadoop/mutisource/account";
        String output = "hdfs://DamHadoop1:9000/user/hadoop/mutisourceouput/result";
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        FileSystem fs = FileSystem.get(new URI(input1),conf);
        Path outPath = new Path(output);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        Job job = new Job(conf, "mutiSource");
        MultipleInputs.addInputPath(job,new Path( input1), TextInputFormat.class, SalesRecordMapper.class);
        MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, AccountRecordMapper.class);

        job.setJarByClass(MutiResource.class);
        job.setReducerClass(MutiRecordReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        int flag = job.waitForCompletion(true) ? 0 : 1;
        Date end_time = new Date();
        System.out.println("job end:" + end_time);
        System.out.println("The job takes:" + (end_time.getTime() - startTime.getTime()) + " ms.");
    }
}
