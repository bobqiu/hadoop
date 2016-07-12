package com.qiu.mptest.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * mapreduce二次排序，支持字符串排序
 * Created by Administrator on 2016/7/7.
 */
public class SecondSort {
    public static class SecondSortMapper extends Mapper<LongWritable, Text, DescSort, Text> {
        private DescSort ds = new DescSort();
        private IntWritable second = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("SecondSortMapper key:value=" + key + ":" + value.toString());
            String[] parts = value.toString().split(";");
            String keyvalue = parts[0];
            String valuevalue = parts[1];
            ds.setFirstKey(keyvalue);
            ds.setSecondKey(valuevalue);

            context.write(ds,new Text(valuevalue));
        }
    }

    public static class SecondSortReducer extends Reducer<DescSort, Text, Text, Text> {
        @Override
        protected void reduce(DescSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            context.write(new Text("====================================================="),null);
            for (Text value : values) {
                context.write(new Text(key.getFirstKey()), value);
            }
        }
    }

    public static class FirstkeyPartitioner extends Partitioner<DescSort, Text> {
        @Override
        public int getPartition(DescSort descSort, Text text, int numPartitions) {
            return Math.abs(descSort.getFirstKey().hashCode() * 127) % numPartitions;
        }
    }

    public static class GroupingComparator extends WritableComparator {
        protected  GroupingComparator() {
            super(DescSort.class, true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            DescSort ds1 = (DescSort) a;
            DescSort ds2 = (DescSort) b;
            String key = ds1.getFirstKey();
            String key2 = ds2.getFirstKey();
            return key.compareTo(key2);
        }

    }

    public static class DescSort implements WritableComparable {
        public DescSort() {
            super();
        }
        private String firstKey;
        private String secondKey;

        public String getFirstKey() {
            return firstKey;
        }

        public void setFirstKey(String firstKey) {
            this.firstKey = firstKey;
        }

        public String getSecondKey() {
            return secondKey;
        }

        public void setSecondKey(String secondKey) {
            this.secondKey = secondKey;
        }

        public int compareTo(Object o) {
            DescSort d= (DescSort) o;
            if (!firstKey.equals(d.firstKey)){
                return firstKey.compareTo(d.firstKey);
            } else if (!secondKey.equals(d.secondKey)) {
                return secondKey.compareTo(d.secondKey);
            }
            return 0;
            //return this.getFirstKey().compareTo(d.getFirstKey());
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(firstKey);
            out.writeUTF(secondKey);
        }

        public void readFields(DataInput in) throws IOException {
            firstKey = in.readUTF();
            secondKey = in.readUTF();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DescSort descSort = (DescSort) o;

            if (firstKey != null ? !firstKey.equals(descSort.firstKey) : descSort.firstKey != null) return false;
            return secondKey != null ? secondKey.equals(descSort.secondKey) : descSort.secondKey == null;

        }

        @Override
        public int hashCode() {
            int result = firstKey != null ? firstKey.hashCode() : 0;
            result = 31 * result + (secondKey != null ? secondKey.hashCode() : 0);
            return result;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        String INPUT_PATH="hdfs://DamHadoop1:9000/user/hadoop/multiout/citysort";
        String OUT_PATH="hdfs://DamHadoop1:9000/user/hadoop/secoudesortout";


        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        Job job = Job.getInstance(conf,"secoundsort job");

        FileSystem fs = FileSystem.get(new URI(INPUT_PATH), conf);
        if (fs.exists(new Path(OUT_PATH))) {
            fs.delete(new Path(OUT_PATH), true);
        }


        job.setMapperClass(SecondSortMapper.class);
        job.setReducerClass(SecondSortReducer.class);
        job.setPartitionerClass(FirstkeyPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setMapOutputKeyClass(DescSort.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job,new Path(OUT_PATH));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
