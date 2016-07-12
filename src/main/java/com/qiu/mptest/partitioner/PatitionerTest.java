package com.qiu.mptest.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/7/6.
 */
public class PatitionerTest {
    public static class PartitionMaper extends Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(";");
            System.out.println("PartitionMaper11 key:value=" + values[0] + ":" + values[1]);
            context.write(new Text(values[0]), new Text(values[1]));
        }
    }

    public static class PartionsPartitioner extends Partitioner<Text, Text> {

              public int getPartition(Text key, Text value, int numPartitions) {
            System.out.println("partionspartitioner11:key:" + key.toString() + ";value:" + value.toString() + ";numpartions:" + numPartitions);
            String keys = key.toString();
            if (keys.equals("bj")) {
                return 1 % numPartitions;
            } else if (keys .equals("sz") ) {
                return 2 % numPartitions;
            } else if (keys.equals("sh")) {
                return 3 % numPartitions;
            }else{
                return 0;
            }

        }

    }

    public static class PartitionsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           // String keys = key.toString().split(",")[0];
            for (Text value : values) {
                System.out.println("PartitionsReducer11 key:value=" + key.toString() + ":" + value);
                context.write(key, value);
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String INPUT_PATH="hdfs://DamHadoop1:9000/user/hadoop/multiout/citycount";
        String OUT_PATH="hdfs://DamHadoop1:9000/user/hadoop/pationout";

        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        Job job = Job.getInstance(conf,"Partition job");

        FileSystem fs = FileSystem.get(new URI(INPUT_PATH),conf);
        Path outPath = new Path(OUT_PATH);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        job.setJarByClass(PatitionerTest.class);


        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(PartitionMaper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(PartionsPartitioner.class);
        job.setNumReduceTasks(4);

        job.setReducerClass(PartitionsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job,new Path(OUT_PATH));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
