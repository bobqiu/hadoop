package com.qiu.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;


public class FreqCounter {
    static class CountMapper extends TableMapper<ImmutableBytesWritable, IntWritable>{
        private static final IntWritable one=new IntWritable(1);
        private int numRecords=0;
        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException,
                InterruptedException {
            // TODO Auto-generated method stub
            ImmutableBytesWritable userKey=new ImmutableBytesWritable(row.get(),0,Bytes.SIZEOF_INT);
            context.write(userKey, one);
            numRecords++;
            if((numRecords%1000)==0){
                context.setStatus("mapper processed"+numRecords+" records so far");
            }
        }
        
    }
    static class CountReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable>{

        protected void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable val:values){
                sum+=val.get();
            }
            // TODO Auto-generated method stub
            Put put=new Put(key.get());
            put.add(Bytes.toBytes("details"),Bytes.toBytes("total"),Bytes.toBytes(sum));
            System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.get()), sum));
            context.write(key, put);
        }
        
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration hbaseConf=HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "DamHadoop1,DamHadoop2,DamHadoop3");//zookeeper服务器地址，要换成自己服务器地址
        //Job jobConf=new Job(hbaseConf, "FreqCounter");
        Job jobConf=Job.getInstance(hbaseConf, "FreqCounter");
        jobConf.setJobName("Hbase_FreqCounter");
        jobConf.setJarByClass(FreqCounter.class);
        
        Scan scan=new Scan();
        String columns="details";
        scan.addFamily(Bytes.toBytes(columns));
        scan.setFilter(new FirstKeyOnlyFilter());
        TableMapReduceUtil.initTableMapperJob("import_tests", scan, CountMapper.class, ImmutableBytesWritable.class,IntWritable.class, jobConf);
        TableMapReduceUtil.initTableReducerJob("summary_user", CountReducer.class, jobConf);
        System.exit(jobConf.waitForCompletion(true) ? 0 : 1);
        
        
    }
}
