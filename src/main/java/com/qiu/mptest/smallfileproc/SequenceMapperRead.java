package com.qiu.mptest.smallfileproc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/7/15.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class SequenceMapperRead {

    static  String INPUTPATH = "hdfs://DamHadoop1:9000//user/hadoop/linktestout/total.seq";
    static  String OUTPATH = "hdfs://DamHadoop1:9000//user/hadoop/SequenceMapperReadout";
    private static FileSystem fileSystem;

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");
        try {
            fileSystem = FileSystem.get(new URI(OUTPATH), conf);
            if (fileSystem.exists(new Path(OUTPATH))) {
                fileSystem.delete(new Path(OUTPATH), true);
            }

            Job job = new Job(conf, SequenceReadMapper.class.getName());

            FileInputFormat.setInputPaths(job, INPUTPATH);

            job.setInputFormatClass(SequenceFileInputFormat.class);

            job.setMapperClass(SequenceReadMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileOutputFormat.setOutputPath(job, new Path(OUTPATH));
            job.setOutputFormatClass(TextOutputFormat.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static class SequenceReadMapper extends Mapper<Text, BytesWritable, Text, Text> {
        private static SequenceFile.Reader reader = null;
        private static Configuration conf = null;
        private Text outValue = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = new Configuration();
            conf.set("HADOOP_USER_NAME","hadoop");
            Path path = new Path(INPUTPATH);
            reader = new SequenceFile.Reader(fileSystem, path, conf);
        }

        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            System.out.println("key:" + key.toString() + ";value:" + value.toString());
            if (!"".equals(key.toString()) && !"".equals(value.get())) {
                outValue.set(new String(value.getBytes(), 0, value.getLength()));
                System.out.println("key:" + key.toString() + ";outValue:" + outValue.toString());
                context.write(key, outValue);
            }
        }
    }
}
