package com.qiu.hbase.importdata.throughmp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/26.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class HBaseImportByMP extends Configured {
    private static final Log LOG = LogFactory.getLog(HBaseImportByMP.class);
    private static final String JOBNAME = "MapReduceImport";

    public static class Map extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
        Configuration conf = null;
        HTable hTable = null;
        static long count = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            String[] values = value.toString().split("\t");
            Put put = new Put(Bytes.toBytes(values[0]));
            put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("value1"), null);
            hTable.put(put);
            if ((++count % 100000) == 0) {
                context.setStatus(count + " doc done!");
                context.progress();
                System.out.println(count + " doc done!");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            hTable.flushCommits();
            hTable.close();
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            conf = context.getConfiguration();
            hTable = new HTable(conf, "testtable2");
            hTable.setAutoFlush(false);
            hTable.setWriteBufferSize(12 * 1024 * 1024);
        }
    }



    public static int main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String input = "";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "DamHadoop1:60000");

        Job job = new Job(conf, JOBNAME);
        job.setJarByClass(HBaseImportByMP.class);
        job.setMapperClass(Map.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.setInputPaths(job, input);
        job.setOutputFormatClass(NullOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
