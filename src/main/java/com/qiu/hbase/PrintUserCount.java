
package com.qiu.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class PrintUserCount {

    /**
     * @param args
     * @throws IOException
     * @throws ZooKeeperConnectionException
     * @throws MasterNotRunningException
     */
    public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        // TODO Auto-generated method stub
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "DamHadoop1,DamHadoop2,DamHadoop3");
        HTable hTable = new HTable(conf, "import_tests");
        //HTable hTable = new HTable(conf, "summary_user");
        Scan scan = new Scan();
        ResultScanner rs = hTable.getScanner(scan);
        for (Result r : rs) {
            for (Cell cell : r.rawCells()) {
                System.out.println("行键："+Bytes.toInt(CellUtil.cloneRow(cell)));
                System.out.println("簇键："+new String(CellUtil.cloneFamily(cell)));
            }
        }
        /*Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        Result r;
        while (((r = scanner.next()) != null)) {
            //ImmutableBytesWritable b = r.getBytes();
            byte[] key = r.getRow();
            int userId = Bytes.toInt(key);
            byte[] totalValue = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("total"));
            int count = Bytes.toInt(totalValue);

            System.out.println("key: " + userId+ ",  count: " + count);
        }
        scanner.close();
        hTable.close();*/
    }

}
