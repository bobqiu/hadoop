package com.qiu.hbase.importdata.throughbaseclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jasper.xmlparser.ParserUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/7/25.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class HBaseHelper {
    private Configuration conf = null;
    private HBaseAdmin hBaseAdmin = null;

    public HBaseHelper(Configuration conf) throws IOException {
        this.conf = conf;
        this.hBaseAdmin = new HBaseAdmin(conf);
    }

    public static HBaseHelper getHelper(Configuration conf) throws IOException {
        return new HBaseHelper(conf);
    }

    public void put(String table, String row, String fam, String qual, long ts, String val) throws IOException {
        HTable hTable = new HTable(conf, table);
        Put put = new Put(Bytes.toBytes(row));
        put.add(Bytes.toBytes(fam), Bytes.toBytes(qual), ts, Bytes.toBytes(val));
        hTable.put(put);
        hTable.close();
    }

    public void put(String table, String[] rows, String[] fams, String[] quals, long[] ts, String[] vals) throws IOException {
        HTable hTable = new HTable(conf, table);
        for (String row : rows) {
            Put put = new Put(Bytes.toBytes(row));
            for (String fam : fams) {
                int v =0;
                for (String qual : quals) {
                    String val = vals[v < vals.length ? v : vals.length];
                    long t = ts[v < ts.length ? v : ts.length];
                    put.add(Bytes.toBytes(fam), Bytes.toBytes(qual), t, Bytes.toBytes(val));
                    v++;
                }
                hTable.put(put);
            }
            hTable.close();

        }
    }

    public void dump(String table, String[] rows, String[] fams, String[] quals) throws IOException {
        HTable hTable = new HTable(conf, table);
        List<Get> list = new ArrayList<Get>();
        for (String row : rows) {
            Get get = new Get(Bytes.toBytes(row));
            if (fams != null) {
                for (String fam : fams) {
                    for (String qual : quals) {
                        get.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qual));
                    }
                }
            }
            list.add(get);
        }
        Result[] results = hTable.get(list);
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println("cell:" + cell + "   value:" + Bytes.toString(cell.getValueArray()));
            }
        }
    }

    public void dropTable(String table) throws IOException {
         if (hBaseAdmin.tableExists(table)) {
            hBaseAdmin.disableTable(table);
            hBaseAdmin.deleteTable(table);
        }
    }

    public void createTable(String table) throws IOException {
        HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf(table));
        if (hBaseAdmin.tableExists(table)) {
             hBaseAdmin.createTable(hTableDesc);
        }
    }
}
