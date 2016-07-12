package com.qiu.mptest.linktest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by Administrator on 2016/7/8.
 */
public class LinkTest {

    private static String INPUTPATH1 = "hdfs://DamHadoop1:9000//user/hadoop/linktest/userinfo";
    private static String INPUTPATH2 = "hdfs://DamHadoop1:9000//user/hadoop/linktest/userlog";
    private static String OUTPATH="hdfs://DamHadoop1:9000//user/hadoop/linktestout";

    public static void main(String[] args) throws URISyntaxException, IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");
        FileSystem fs = FileSystem.get(new URI(OUTPATH), conf);
        if (fs.exists(new Path(OUTPATH))) {
            fs.delete(new Path(OUTPATH), true);
        }
        Job job = new Job(conf, LinkTest.class.getName());

        job.getConfiguration().set("joinType", "allOuter");

        MultipleInputs.addInputPath(job, new Path(INPUTPATH1), TextInputFormat.class, UserInfoMapper.class);
        MultipleInputs.addInputPath(job, new Path(INPUTPATH2), TextInputFormat.class, UserLogMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserLog.class);

        job.setReducerClass(UserReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(OUTPATH));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class UserLogMapper extends Mapper<LongWritable, Text, Text, UserLog> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            Text userId = new Text(values[0]);
            context.write(userId, new UserLog("L", value.toString()));
        }
    }

    public static class UserInfoMapper extends Mapper<LongWritable, Text, Text, UserLog> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            Text userId = new Text(values[0]);
            context.write(userId, new UserLog("I", value.toString()));
        }
    }

    public static class UserReduce extends Reducer<Text, UserLog, Text, Text> {
        private ArrayList<Text> users = new ArrayList<Text>();
        private ArrayList<Text> logs = new ArrayList<Text>();
        private String joinType;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.joinType = context.getConfiguration().get("joinType");
            System.out.println("UserReduce setup joinType:" + joinType);

        }

        @Override

        protected void reduce(Text key, Iterable<UserLog> values, Context context) throws IOException, InterruptedException {
            users.clear();
            logs.clear();
            for (UserLog userLog : values) {
                System.out.println("UserReduce reduce :" + key + "==" + userLog.toString());
                if (userLog.getType() .equals("I") ) {
                    users.add(new Text(userLog.getData()));
                } else {
                    logs.add(new Text(userLog.getData()));
                }
            }
            System.out.println("user size:" + users.size() + ";log size:" + logs.size());
            if (joinType.equals("innerJoin")) {
                if (users.size() > 0 && logs.size() > 0) {
                    for (Text user : users) {
                        for (Text log : logs) {
                            context.write(user, log);
                        }
                    }
                }
            } else if (joinType.equals("leftOuter")) {
                    for (Text user : users) {
                        if (logs.size() > 0) {
                            for (Text log : logs) {
                                context.write(user, log);
                            }
                        } else {
                            context.write(user, createEmptyLog());
                        }
                    }
                } else if (joinType.equals("rightOuter")) {
                    for (Text log : logs) {
                        if (users.size() > 0) {
                            for (Text user : users) {
                                context.write(user, log);
                            }
                        }else{
                            context.write( createEmptyLog(),log);
                        }
                    }
                } else if (joinType.equals("allOuter")) {
                    if (users.size() > 0) {
                        for (Text user : users) {
                            if (logs.size() > 0) {
                                for (Text log : logs) {
                                    context.write(user, log);
                                }
                            } else {
                                context.write(user, createEmptyLog());
                            }
                        }
                    } else {
                        for (Text log : logs) {
                            if (users.size() > 0) {
                                for (Text user : users) {
                                    context.write(user, log);
                                }
                            }else {
                                context.write(createEmptyLog(), log);
                            }
                        }
                }
            }
        }

        private Text createEmptyLog() {
            return new Text("NULL");
        }
    }



    private static class UserLog implements Writable{
        private String type;
        private String data;

        public UserLog() {
        }

        public UserLog(String type, String data) {

            this.type = type;
            this.data = data;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(type);
            out.writeUTF(data);
        }

        public void readFields(DataInput in) throws IOException {
            this.type = in.readUTF();
            this.data = in.readUTF();
        }

        @Override
        public String toString() {
            return "UserLog{" +
                    "type='" + type + '\'' +
                    ", data='" + data + '\'' +
                    '}';
        }
    }
}
