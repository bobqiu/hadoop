/*
 * @(#)HiveHbaseTest.java
 * 
 * CopyRight (c) 2014 保留所有权利。
 */

package com.qiu.hivetest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Title : HiveHbaseTest
 * <p/>
 * Description :
 * <p/>
 * CopyRight : CopyRight (c) 2014
 * <p/>
 * Company :
 * <p/>
 * JDK Version Used : JDK 5.0 +
 * <p/>
 * Modification History :
 * <p/>
 * 
 * <pre>
 * NO.    Date    Modified By    Why & What is modified
 * </pre>
 * 
 * <pre>
 * 1    2014-9-8    qiubo        Created
 * </pre>
 * <p/>
 * 
 * @author qiubo
 * @version 1.0.0.2014-9-8
 */
public class HiveHbaseImport {

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://10.1.251.122:10000/hivetest", "hadoop", "hadoop");
        Statement stmt = con.createStatement();
        StringBuilder sb= new StringBuilder();
        
       /* sb=new StringBuilder("CREATE TABLE test_hbase_20140402(keyss string, value string) PARTITIONED BY(a string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,cf1:val\") TBLPROPERTIES (\"hbase.table.name\" = \"aaa_20140402\")");
        System.out.println("执行创建表开始：SQL="+sb.toString());
        stmt.execute(sb.toString());
        System.out.println("执行创建表结束：SQL="+sb.toString());*/
        
        sb=new StringBuilder(" insert overwrite table test_hbase_20140402 PARTITION(a='1') select '2','2' from tas_app_age_gprs_20140905 limit 10");
        System.out.println("执行导入数据开始：sql="+sb.toString());
        stmt.execute(sb.toString());
        System.out.println("执行导入数据完成");
        
        stmt.close();
        con.close();
    }
}
