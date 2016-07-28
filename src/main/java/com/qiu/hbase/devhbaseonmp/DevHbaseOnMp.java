package com.qiu.hbase.devhbaseonmp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/27.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class DevHbaseOnMp {

    private static final byte[] INDEX_COLUMN = Bytes.toBytes("INDEX");
    private static final byte[] INDEX_QUALIFIER = Bytes.toBytes("ROW");

    public static class IndexBuildMap extends Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put> {
        private HashMap<byte[], ImmutableBytesWritable> indexes;
        private byte[] family;

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            System.out.println("indexvalue:"+indexes.toString()+"   isempty:"+indexes.isEmpty());
            for (Map.Entry<byte[], ImmutableBytesWritable> index : indexes.entrySet()) {
                byte[] qualifier = index.getKey();
                ImmutableBytesWritable tableName = index.getValue();

                byte[] newValue = value.getValue(family, qualifier);

                if (newValue != null) {
                    Put put = new Put(newValue);
                    put.add(INDEX_COLUMN, INDEX_QUALIFIER, key.get());
                    context.write(tableName, put);
                }
            }

        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] fields = conf.getStrings("index.fields");
            family = Bytes.toBytes(conf.get("index.familyname"));
            String tableName = conf.get("index.tableName");
            indexes = new HashMap<byte[], ImmutableBytesWritable>();
            for (String field : fields) {
                System.out.println("indexesvalue:" + field + "  tableName:" + tableName);
                indexes.put(Bytes.toBytes(field), new ImmutableBytesWritable(Bytes.toBytes(tableName + "-" + field)));
            }
        }


    }
    public static void initHtable(Configuration conf, String tableName) throws IOException {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        hTableDescriptor.addFamily(hColumnDescriptor);

        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);

        if (hBaseAdmin.tableExists(tableName)) {
            System.out.println("数据表" + tableName + "已存在，重新创建");
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        }

        hBaseAdmin.createTable(hTableDescriptor);

        HTable table = new HTable(conf, tableName);

        System.out.println("向表zzts【" + tableName + "】插入数据。");

        // 添加数据
        addRow(table, "1", "info", "name", "peter");
        addRow(table, "1", "info", "email", "peter@heroes.com");
        addRow(table, "1", "info", "power", "absorb abilities");

        addRow(table, "2", "info", "name", "hiro");
        addRow(table, "2", "info", "email", "hiro@heroes.com");
        addRow(table, "2", "info", "power", "bend time and space");

        addRow(table, "3", "info", "name", "sylar");
        addRow(table, "3", "info", "email", "sylar@heroes.com");
        addRow(table, "3", "info", "power", "hnow how things work");

        addRow(table, "4", "info", "name", "claire");
        addRow(table, "4", "info", "email", "claire@heroes.com");
        addRow(table, "4", "info", "power", "heal");

        addRow(table, "5", "info", "name", "noah");
        addRow(table, "5", "info", "email", "noah@heroes.com");
        addRow(table, "5", "info", "power", "cath the people with ablities");
    }

    private static void addRow(HTable table, String row, String columnFamily, String column, String value) throws InterruptedIOException,  RetriesExhaustedWithDetailsException {
        Put put = new Put(Bytes.toBytes(row));
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
    }
    private static Job configureJob(Configuration conf,String jobName) throws IOException {
        Job job = new Job(conf, jobName);
        job.setJarByClass(DevHbaseOnMp.class);
        job.setMapperClass(IndexBuildMap.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(MultiTableOutputFormat.class);
        return job;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "DamHadoop1,DamHadoop2,DamHadoop3");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        String tableName = "User";
        String columnFamily = "info";
        String[] fields = {"name", "power"};

        DevHbaseOnMp.initHtable(conf, tableName);

        for (String field : fields) {
            System.out.println("========================wsx tableName:"+tableName+";field:"+field+"==========================");
            DevHbaseOnMp.createIndexTable(conf, tableName + "-" + field);
        }

        conf.set(TableInputFormat.SCAN, couvertScanToString(new Scan()));
        //conf.set(TableInputFormat.SCAN, couvertScanToString(new Scan()));
        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        conf.set("index.tableName", tableName);
        conf.set("index.familyname", columnFamily);
        conf.setStrings("index.fields", fields);

        Job job = DevHbaseOnMp.configureJob(conf, "INDEX_BUILDER");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

  public static String couvertScanToString(Scan scan) throws IOException {
       /* ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());*/
      ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
      return Base64.encodeBytes(proto.toByteArray());
    }

    private static void createIndexTable(Configuration conf, String tablename) throws IOException {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(INDEX_COLUMN);
        hTableDescriptor.addFamily(hColumnDescriptor);

        if (hBaseAdmin.tableExists(tablename)) {
            hBaseAdmin.disableTable(tablename);
            hBaseAdmin.deleteTable(tablename);
        }
        hBaseAdmin.createTable(hTableDescriptor);
        System.out.println("创建表【"+tablename+"】成功");
    }
}
