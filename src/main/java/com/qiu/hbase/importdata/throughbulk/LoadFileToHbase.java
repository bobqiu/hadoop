package com.qiu.hbase.importdata.throughbulk;

import com.qiu.hbase.importdata.throughbaseclient.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;


/**
 * Created by Administrator on 2016/7/26.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 * $HADOOP_HOME/bin/hadoop fs -mkdir /user/test
 * 创建数据表
 * create 'student', {NAME => 'info'}
 * 调用 importtsv 命令导入数据，
 * $HADOOP_HOME/bin/hadoop jar /usr/lib/cdh/hbase/hbase-0.94.15-hdh4.6.0.jar
 * importtsv -Dimporttsv.columns=HBASE_ROW_KEY,info:name,info:age,info:phone
 * -Dimporttsv.bulk.output=/user/test/output/ student /user/test/data.tsv
 *
 * 创建生成文件的文件夹：
 * $HADOOP_HOME/bin/hadoop fs -mkdir /user/hac/output
 * 开始导入数据：
 * $HADOOP_HOME/bin/hadoop jar /usr/lib/cdh/hbase/hbase-0.94.15-hdh4.6.0.jar
 * importtsv -Dimporttsv.bulk.output=/user/hac/output/2-1 -Dimporttsv.columns=
 * HBASE_ROW_KEY,info:name,info:age,info:phone student /user/hac/input/2-1
 * 完成 bulk load 导入
 * $HADOOP_HOME/bin/hadoop jar /usr/lib/cdh/hbase/hbase-0.94.15-hdh4.6.0.jar
 * completebulkload /user/hac/output/2-1 student
 */
public class LoadFileToHbase {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper = HBaseHelper.getHelper(conf);

        helper.dropTable("testtable2");
        helper.createTable("testtable2");
        HTable hTable = new HTable(conf, "testtable2");
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path("11"), hTable);

    }
}
