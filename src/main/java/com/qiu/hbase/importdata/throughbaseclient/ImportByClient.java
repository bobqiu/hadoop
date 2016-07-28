package com.qiu.hbase.importdata.throughbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/25.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class ImportByClient {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        int count = 0;
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        System.out.println("helper:" + helper);
        helper.dropTable("testtable");
        helper.createTable("testtable");

        HTable hTable = new HTable(conf, "testtable");
        long start = System.currentTimeMillis();
        for(int i=1;i<100000;i++) {
            Put put = new Put(Bytes.toBytes("row" + i));
            put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
            put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"));
            hTable.put(put);
            count++;
            if (count % 100000 == 0) {
                System.out.println("completed 100000 rows insertion");
            }
        }
        System.out.print("ImportByClient spend :  ");
        System.out.println( System.currentTimeMillis() - start);
    }
}
