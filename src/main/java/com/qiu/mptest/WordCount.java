/*
 * @(#)WordCount.java
 * 
 * CopyRight (c) 2014 保留所有权利。
 */

package com.qiu.mptest;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


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
public class WordCount {

   public static class WordCountMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<Object, Text, Text, IntWritable>{
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
   
   public static class WordCountReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, IntWritable, Text, IntWritable>{
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
        
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("WordCount");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCountMapper.class);
        conf.setCombinerClass(WordCountReducer.class);
        conf.setReducerClass(WordCountReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
   }
}
