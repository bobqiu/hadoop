package com.qiu.mptest.smallfileproc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/7/15.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class SequenceMapperWrite {
    private static final String inputPath = "hdfs://DamHadoop1:9000//user/hadoop/linktest";
    private static final String outputPath = "hdfs://DamHadoop1:9000//user/hadoop/linktestout";

    private static FileSystem fileSystem = null;

    public static void main(String[] args) throws URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");

        try {
            fileSystem = FileSystem.get(new URI(outputPath), conf);
            if (fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }

            Job job = new Job(conf, SequenceMapperWrite.class.getName());

            FileInputFormat.setInputPaths(job, inputPath);
            job.setMapperClass(SequenceWriteMapper.class);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static class SequenceWriteMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
        private static SequenceFile.Writer writer = null;
        private Text outkey = new Text();
        private BytesWritable outValue = new BytesWritable();

        private FileStatus[] files = null;

        private InputStream inputstream = null;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = new Configuration();
            conf.set("HADOOP_USER_NAME","hadoop");

            Path path = new Path(inputPath);
            writer = SequenceFile.createWriter(fileSystem, conf, new Path(outputPath + "/total.seq"), Text.class, BytesWritable.class, SequenceFile.CompressionType.BLOCK, new BZip2Codec());
            files = fileSystem.listStatus(path);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("======================files.length:" + files.length);
            for(int i=0;i<files.length;i++) {
                outkey.set(files[i].getPath().toString());
                System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$outkey:" + outkey.toString() + "==outvalue:" + outValue.toString());

                inputstream = fileSystem.open(files[i].getPath());
                byte[] buffer = new byte[(int) files[i].getLen()];
                IOUtils.readFully(inputstream, buffer, 0, buffer.length);
                outValue.set(new BytesWritable(buffer));

                IOUtils.closeStream(inputstream);
                writer.append(outkey, outValue);
            }
            IOUtils.closeStream(writer);

        }
    }
}
