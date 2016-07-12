package com.qiu.mptest.temperature;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Title :
 * <p/>
 * Description :
 * <p/>
 * CopyRight : CopyRight (c) 2014
 * <p/>
 * <p/>
 * JDK Version Used : JDK 5.0 +
 * <p/>
 * Modification History	:
 * <p/>
 * <pre>NO.    Date    Modified By    Why & What is modified</pre>
 * <pre>1    14.12.14    bob        Created</pre>
 * <p/>
 *
 * @author bob
 */
public class Temperature {
    private  final static  Log log= LogFactory.getLog(Temperature.class);
    private static String INPUT_PATH="hdfs://DamHadoop1:9000/user/hadoop/infile";
    private static String OUT_PATH="hdfs://DamHadoop1:9000/user/hadoop/out";

    static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        /**
         * Called once for each key/value pair in the input split. Most applications
         * should override this, but the default is the identity function.
         *
         * @param key
         * @param value
         * @param context
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            log.debug("befor mapper:key:"+key+";value:"+value);
            String line=value.toString();
            String year = line.substring(0, 4);
            int template = Integer.parseInt(line.substring(8));
            context.write(new Text(year), new IntWritable(template));
            log.debug("after mapper:key:"+new Text(year)+";value:"+new IntWritable(template));
        }
    }

    static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /**
         * This method is called once for each key. Most applications will define
         * their reduce class by overriding this method. The default implementation
         * is an identity function.
         *
         * @param key
         * @param values
         * @param context
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxValue = Integer.MAX_VALUE;
            StringBuffer sb = new StringBuffer();
            for (IntWritable value : values) {
                maxValue = Math.min(maxValue, value.get());
                sb.append(value).append(",");
            }
            log.debug("key:"+key.toString()+";value:"+sb.toString());
            context.write(key,new IntWritable(maxValue));
            log.debug("key:"+key.toString()+";maxValue:"+maxValue);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        Job job = Job.getInstance(conf,"temperature job");

        FileSystem fs = FileSystem.get(new URI(INPUT_PATH),conf);
        Path outPath = new Path(OUT_PATH);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(TempMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job,new Path(OUT_PATH));


        job.waitForCompletion(true);
    }
}
