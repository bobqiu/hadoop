package com.qiu.hbase.importdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hdfs.protocol.proto.NamenodeProtocolProtos;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/25.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class GenerateHFile {
    private static final String INPUT = "";
    private static final String OUTPUT = "";


    public static class GenerateHFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split(",", -1);
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(items[0].getBytes());
            KeyValue kvProtocol = new KeyValue(items[0].getBytes(), "colfam1".getBytes(), "colfam1".getBytes(), items[0].getBytes());
            if (null != kvProtocol) {
                context.write(rowKey, kvProtocol);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();

        HTable table = new HTable(conf, "HtableTest");

        Job job = new Job(conf, "GenerateHFile");
        job.setJarByClass(GenerateHFile.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);
        job.setMapperClass(GenerateHFileMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        HFileOutputFormat2.configureIncrementalLoad(job, table);
        FileInputFormat.addInputPath(job, new Path(INPUT));
        FileOutputFormat.setOutputPath(job,new Path(OUTPUT));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
