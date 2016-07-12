package com.qiu.mptest.cfm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/7/4.
 */
public class UniqueListener {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String input = "hdfs://DamHadoop1:9000//user/hadoop/cfmdata";
        String output = "hdfs://DamHadoop1:9000/user/hadoop/cfmoutput/result";
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        FileSystem fs = FileSystem.get(new URI(input),conf);
        Path outPath = new Path(output);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        Job job = Job.getInstance(conf,"cfm uniqueListener job");
        job.setJobName("cfm uniqueListener");
        /* job.setMapperClass(UniqueListenerMapper.class);
        job.setInputFormatClass(TextInputFormat.class);

    //    job.setCombinerClass(UniqueListenerReducer.class);
        job.setReducerClass(UniqueListenerReducer.class);
        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);*/
        job.setMapperClass(MergeListenerMapper.class);
        job.setMapperClass(SumMapper.class);

        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(TrackStats.class);

        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        job.waitForCompletion(true);
       // System.exit(0);

    }
}
