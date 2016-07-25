package com.qiu.mptest.smallfileproc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/7/15.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class ConvertSmallToSequenceFile {
    private static final String inputPath = "hdfs://DamHadoop1:9000//user/hadoop/linktest";
    private static final String outputPath = "hdfs://DamHadoop1:9000//user/hadoop/ConvertSmallToSequenceFileout";


    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            FileSystem filesystem = FileSystem.get(new URI(inputPath), conf);

            if (filesystem.exists(new Path(outputPath))) {
                filesystem.delete(new Path(outputPath), true);
            }

            Job job = new Job(conf, ConvertSmallToSequenceFile.class.getName());

            FileInputFormat.addInputPaths(job, inputPath);
            job.setInputFormatClass(WholeFileInputFormat.class);

            job.setMapperClass(SequenceFileMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(BytesWritable.class);

            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);

            System.exit(job.waitForCompletion(true)?0:1);

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

    private static class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text fileNameKey = null;
        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            System.out.println(fileNameKey.toString());
            context.write(fileNameKey, value);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            InputSplit split = context.getInputSplit();
            Path path=((FileSplit)split).getPath();
            fileNameKey = new Text(path.toString());
        }
    }
}
