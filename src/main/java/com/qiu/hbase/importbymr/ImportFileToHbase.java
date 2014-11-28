package com.qiu.hbase.importbymr;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class ImportFileToHbase {
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            Configuration hconf=HBaseConfiguration.create();
            hconf.set("hbase.zookeeper.quorum", "DamHadoop1,DamHadoop2,DamHadoop3");//zookeeper服务器地址，要换成自己服务器地址
            String[] pages={"/","/a.html","/b.html","/c.html"};
            HTable hTable=new HTable(hconf, "import_tests");
            hTable.setAutoFlush(false, true);
            hTable.setWriteBufferSize(1024*1024*12);
            
            int totalRecords=100000;
            int maxID=totalRecords/1000;
            Random rand=new Random();
            System.out.println("导入"+totalRecords+"条记录！");
            for(int i=0;i<totalRecords;i++){
                int userId=rand.nextInt(maxID)+1;
               // int rowKey=userId+i;
                byte[] rowKey=Bytes.add(Bytes.toBytes(userId), Bytes.toBytes(i));
                
                String randomPage=pages[rand.nextInt(pages.length)];
               // Put put=new Put(Bytes.toBytes(rowKey));
                Put put=new Put(rowKey);
                put.add(Bytes.toBytes("details"),Bytes.toBytes("page"),Bytes.toBytes(randomPage));
                hTable.put(put);
            }
            hTable.flushCommits();
            hTable.close();
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
