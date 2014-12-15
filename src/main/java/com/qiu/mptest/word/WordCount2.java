/*
 * @(#)WordCount.java
 * 
 * CopyRight (c) 2014 保留所有权利。
 */

package com.qiu.mptest.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


/**
 * Title : WordCount
 * <p/>
 * Description :
 * <p/>
 * CopyRight : CopyRight (c) 2014
 * <p/>
 * Company :
 * <p/>
 * JDK Version Used : JDK 5.0 +
 * <p/>
 * Modification History :
 * <p/>
 * 
 * <pre>
 * NO.    Date    Modified By    Why & What is modified
 * </pre>
 * 
 * <pre>
 * 1    2014-9-18    qiubo        Created
 * </pre>
 * <p/>
 * 
 * @author qiubo
 * @version 1.0.0.2014-9-18
 */
public class WordCount2 {

   public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
       private final static IntWritable one =new IntWritable(1);
       private Text word=new Text();
    /**
     * {@inheritDoc}
     * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
     */
    public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException {
        // TODO Auto-generated method stub
        StringTokenizer itr=new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            output.collect(word, one);
        }
    }
       
   }
   
   public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
       private IntWritable result=new IntWritable();
    /**
     * {@inheritDoc}
     * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
     */
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
            Reporter reporter) throws IOException {
        // TODO Auto-generated method stub
        int sum =0 ;
        while(values.hasNext()){
            sum+=values.next().get();
        }
        result.set(sum);
        output.collect(key, result);
    }
       
   }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://DamHadoop1:9000/user/hadoop/in";
        String output = "hdfs://DamHadoop1:9000/user/hadoop/output/result";
        Configuration conf=new Configuration();
        Job job = Job.getInstance(conf,"wordCount");
        
        job.setJarByClass(WordCount2.class);
     
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

      //  JobClient.runJob(job);
        job.waitForCompletion(true);
        System.exit(0);
       
   }
}

/**
 * 
 * package com.gqshao.hadoop.remote;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class WordCount extends Configured implements Tool {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        this.getClass().getResource("/hadoop/");
        Configuration conf = getConf();
        Job job = new Job(conf);
        conf.set("mapred.job.tracker", "192.168.0.128:9001");
        conf.set("fs.default.name", "hdfs://192.168.0.128:9000");
        conf.set("hadoop.job.ugi", "hadoop");
        conf.set("Hadoop.tmp.dir", "/user/gqshao/temp/");

        job.setJarByClass(WordCount.class);
        job.setJobName("wordcount");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String hdfs = "hdfs://192.168.0.128:9000";
        args = new String[] { hdfs + "/user/gqshao/input/big", hdfs + "/user/gqshao/output/WordCount/" + new Date().getTime() };
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WordCount(), args);
        System.exit(ret);
    }
}
**/
  
