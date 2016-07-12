
package com.qiu.hbase.importbymr;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

public class PrintUserCount {

    private static final Log log= LogFactory.getLog(PrintUserCount.class);

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
               /* log.debug("行键：" + Bytes.toInt(CellUtil.cloneRow(cell)));
                log.debug("簇键：" + new String(CellUtil.cloneFamily(cell)));*/
                log.debug("");
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

            log.debug("key: " + userId+ ",  count: " + count);
        }
        scanner.close();
        hTable.close();*/
    }

}
