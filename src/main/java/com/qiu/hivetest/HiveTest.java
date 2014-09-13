/*
 * @(#)HiveTest.java
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
 * Title : HiveTest
 * <p/>
 * Description : 创建表，并导入数据
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
 * 1    2014-9-5    qiubo        Created
 * </pre>
 * <p/>
 * 
 * @author qiubo
 * @version 1.0.0.2014-9-5
 */
public class HiveTest {

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://10.1.251.122:10000/hivetest", "hadoop", "hadoop");
        Statement stmt = con.createStatement();
        //删除表DROP TABLE [IF EXISTS] table_name
        String sqlDel="DROP TABLE IF EXISTS SOURCE_TAS_APP_AGE_GPRS_20140905";
        stmt.execute(sqlDel);
        
         sqlDel="DROP TABLE IF EXISTS tas_app_age_gprs_20140905";
        stmt.execute(sqlDel);
        //创建表
       StringBuilder sb=new StringBuilder("CREATE TABLE IF NOT EXISTS SOURCE_TAS_APP_AGE_GPRS_20140905(");
       sb.append("STATIS_MONTH STRING");
       sb.append(",BUSI_ID STRING");
       sb.append(",KEY_WORD STRING");
       sb.append(",SECTION_ID string");
       sb.append(",BRAND_ID STRING");
       sb.append(",WEB_ADDRESS STRING");
       sb.append(")");
       sb.append(" comment '用户应用表'");
       sb.append(" partitioned by (MONTH_ID STRING)");
       sb.append(" row format delimited");
       sb.append(" fields terminated by '\t'");
       //sb.append(" stored as sequencefile");
       sb.append(" stored as textfile");
        
       String sql = sb.toString();
       System.out.println("Running: " + sql);
       stmt.execute(sql);

       //导入数据 load data local inpath '/httplog.txt' into table dq_httplog;
       // sql = "load data inpath '/tmp/testone.txt' into table target_tas_app_age_gprs_20140905" ;
       sql = "load data inpath '/tmp/testhanseq.txt' into table SOURCE_TAS_APP_AGE_GPRS_20140905 partition(MONTH_ID='20:15:59')" ;
       System.out.println("Running: " + sql);
       stmt.execute(sql);
      
       //添加统计信息
       sql = "ANALYZE TABLE SOURCE_TAS_APP_AGE_GPRS_20140905 PARTITION(MONTH_ID) COMPUTE STATISTICS" ;
       System.out.println("Running: " + sql);
       stmt.execute(sql);
       //创建目标表带索引
       StringBuilder sbTarget=new StringBuilder("CREATE TABLE IF NOT EXISTS tas_app_age_gprs_20140905(");
       sbTarget.append("STATIS_MONTH STRING");
       sbTarget.append(",BUSI_ID STRING");
       sbTarget.append(",KEY_WORD STRING");
       sbTarget.append(",SECTION_ID string");
       sbTarget.append(",BRAND_ID STRING");
       sbTarget.append(",WEB_ADDRESS STRING");
       sbTarget.append(")");
       sbTarget.append(" comment '用户应用表'");
       sbTarget.append(" partitioned by (MONTH_ID STRING)");
       sbTarget.append(" row format delimited");
       sbTarget.append(" fields terminated by '\t'");
       sbTarget.append(" stored as sequencefile");
       //sb.append(" stored as textfile");
       String sql1 = sbTarget.toString();
       System.out.println("Running 目标表: " + sql1);
       stmt.execute(sql1);
       //目标表中插入数据
       //StringBuilder hSqlBuffer=new StringBuilder("INSERT OVERWRITE TABLE tas_app_age_gprs_20140905 partition(MONTH_ID='2014-09-10') ");
       StringBuilder hSqlBuffer=new StringBuilder("INSERT OVERWRITE TABLE tas_app_age_gprs_20140905 partition(MONTH_ID='2014-09-10') ");
       hSqlBuffer.append(" select STATIS_MONTH,BUSI_ID,KEY_WORD,SECTION_ID,BRAND_ID,WEB_ADDRESS from SOURCE_TAS_APP_AGE_GPRS_20140905");
       //hSqlBuffer.append(" select '2014-09-10',BUSI_ID,KEY_WORD,SECTION_ID,BRAND_ID,WEB_ADDRESS from SOURCE_TAS_APP_AGE_GPRS_20140905");
       String hSql = hSqlBuffer.toString();
       System.out.println("从源表中导入数据到 目标表: " + hSql);
       stmt.execute(hSql);
       //显示表
       sql = "show tables" ;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
          System.out.println(res.getString(1));
        }
        //sql = "select * from tas_app_age_gprs_20140905" ;
        sql = "select STATIS_MONTH,BUSI_ID,KEY_WORD,SECTION_ID,BRAND_ID,WEB_ADDRESS from tas_app_age_gprs_20140905" ;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
          //System.out.println(res.getString(1)+"1\t"+res.getString(2)+"2\t"+res.getString(3)+"3\t"+res.getString(4)+"4\t"+res.getString(5)+"5\t"+res.getString(6));
            System.out.println(res.getString(1)+"\t"+res.getString(2)+"\t"+res.getString(3)+"\t"+res.getString(4)+"\t"+res.getString(5)+"\t"+res.getString(6));
        }
        //con.commit();
        res.close();
        stmt.close();
        con.close();
    }
}
