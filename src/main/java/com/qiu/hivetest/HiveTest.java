/*
 * @(#)HiveTest.java
 *
 * CopyRight (c) 2014 保留所有权利。
 */

package com.qiu.hivetest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.DriverManager;
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
         private final static Log log= LogFactory.getLog(HiveTest.class);
    /**
     * Description:　创建表： CREATE TABLE page_view( viewTime INT, userid BIGINT,
     * page_url STRING, referrer_url STRING, ip STRING COMMENT 'IP Address of
     * the User') COMMENT 'This is the page view table' PARTITIONED BY(dt
     * STRING, country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
     * COLLECTION ITEMS TERMINATED BY '\002' MAP KEYS TERMINATED BY '\003'
     * STORED AS TEXTFILE;
     * [ROW FORMAT DELIMITED]关键字，是用来设置创建的表在加载数据的时候，支持的列分隔符。不同列之间用一个'\001'分割,集合(例如array,map)的元素之间以'\002'隔开,map中key和value用'\003'分割。
[STORED AS file_format]关键字是用来设置加载数据的数据类型,默认是TEXTFILE，如果文件数据是纯文本，就是使用 [STORED AS TEXTFILE]，然后从本地直接拷贝到HDFS上，hive直接可以识别数据。
     *
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://10.1.251.122:10000/hivetest", "hadoop", "hadoop");
        Statement stmt = con.createStatement();

       /* String delString="drop table if exists  test_hbase_20140902,drop table if exists  test_hbase_20140402,drop table if exists  test_hbase4,drop table if exists  test_hbase3,drop table if exists  tas_app_age_gprs_20140905_no_partition,drop table if exists  tas_app_age_gprs_20140905,drop table if exists  source_tas_app_age_gprs_20141005,drop table if exists  source_tas_app_age_gprs_20140905_no_partition,drop table if exists  source_tas_app_age_gprs_20140905,drop table if exists  hive_tas_app_age_gprs_20140907,drop table if exists  hive_tas_app_age_gprs_20140906,drop table if exists  hive_tas_app_age_gprs_20140905,drop table if exists  hive_tas_app_age_gprs_20140904,drop table if exists  hive_tas_app_age_gprs_20140903,drop table if exists  hive_tas_app_age_gprs_20140902,drop table if exists  hive_tas_app_age_gprs_20140901,drop table if exists  hive_tas_app_age_gprs_20140805,drop table if exists  hive_tas_app_age_gprs_20140804,drop table if exists  hive_tas_app_age_gprs_20140803,drop table if exists  hive_tas_app_age_gprs_20140802,drop table if exists  hive_tas_app_age_gprs_20140801,drop table if exists  hive_tas_app_age_gprs_20140506,drop table if exists  hive_tas_app_age_gprs_20140505,drop table if exists  hive_tas_app_age_gprs_20140503,drop table if exists  hive_tas_app_age_gprs_20140502,drop table if exists  hive_tas_app_age_gprs_20140501,drop table if exists  hive_tas_app_age_gprs_20140407,drop table if exists  hive_tas_app_age_gprs_20140406,drop table if exists  hive_tas_app_age_gprs_20140405,drop table if exists  hive_tas_app_age_gprs_20140403,drop table if exists  hive_tas_app_age_gprs_20140402,drop table if exists  hive_tas_app_age_gprs_20140401,drop table if exists  hbase_table_1";
        String[] arrayString=delString.split("\\,");
        System.out.println(arrayString.length);
        for (int i=0;i<arrayString.length;i++){
            stmt.execute(arrayString[i]);
        }*/


        //删除表DROP TABLE [IF EXISTS] table_name
       /* String sqlDel="DROP TABLE IF EXISTS SOURCE_TAS_APP_AGE_GPRS_20140905";
        stmt.execute(sqlDel);
        
         sqlDel="DROP TABLE IF EXISTS tas_app_age_gprs_20140905";
        stmt.execute(sqlDel);*/
        //创建表，如果load的是文本文件，则需要stored为textfile,如果需要stored为sequencefile则需要通过textfile转换。
       StringBuilder sb=new StringBuilder("CREATE TABLE IF NOT EXISTS SOURCE_TAS_APP_AGE_GPRS_20141005_multirange(");
       sb.append("STATIS_MONTH STRING");
       sb.append(",BUSI_ID STRING");
       sb.append(",KEY_WORD STRING");
       sb.append(",SECTION_ID STRING");
       sb.append(",BRAND_ID STRING");
       sb.append(",WEB_ADDRESS STRING");
       sb.append(")");
       sb.append(" comment '用户应用表'");
       sb.append(" partitioned by (MONTH_ID STRING,BUSIS_ID STRING)");
       sb.append(" row format delimited");
       sb.append(" fields terminated by '\t'");
       //sb.append(" stored as sequencefile");
       sb.append(" stored as textfile");
        
       String sql = sb.toString();
       log.debug("Running: " + sql);
       stmt.execute(sql);

       //导入数据 load data local inpath '/httplog.txt' into table dq_httplog;
       // sql = "load data inpath '/tmp/testone.txt' into table target_tas_app_age_gprs_20140905" ;
    /*   sql = "load data inpath '/tmp/testhanseq.txt' into table SOURCE_TAS_APP_AGE_GPRS_20140905 partition(MONTH_ID='20:15:59')" ;
       log.debug("Running: " + sql);
       stmt.execute(sql);

       //添加统计信息
       sql = "ANALYZE TABLE SOURCE_TAS_APP_AGE_GPRS_20140905 PARTITION(MONTH_ID) COMPUTE STATISTICS" ;
       log.debug("Running: " + sql);
       stmt.execute(sql);
       //创建目标表带索引
       StringBuilder sbTarget=new StringBuilder("CREATE TABLE IF NOT EXISTS tas_app_age_gprs_20140905(");
       sbTarget.append("STATIS_MONTH STRING");
       sbTarget.append(",BUSI_ID STRING");
       sbTarget.append(",KEY_WORD STRING");
       sbTarget.append(",SECTION_ID STRING");
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
       log.debug("Running 目标表: " + sql1);
       stmt.execute(sql1);
       //目标表中插入数据
       //StringBuilder hSqlBuffer=new StringBuilder("INSERT OVERWRITE TABLE tas_app_age_gprs_20140905 partition(MONTH_ID='2014-09-10') ");
       StringBuilder hSqlBuffer=new StringBuilder("INSERT OVERWRITE TABLE tas_app_age_gprs_20140905 partition(MONTH_ID='2014-09-10') ");
       hSqlBuffer.append(" select STATIS_MONTH,BUSI_ID,KEY_WORD,SECTION_ID,BRAND_ID,WEB_ADDRESS from SOURCE_TAS_APP_AGE_GPRS_20140905");
       //hSqlBuffer.append(" select '2014-09-10',BUSI_ID,KEY_WORD,SECTION_ID,BRAND_ID,WEB_ADDRESS from SOURCE_TAS_APP_AGE_GPRS_20140905");
       String hSql = hSqlBuffer.toString();
       log.debug("从源表中导入数据到 目标表: " + hSql);
       stmt.execute(hSql);
       //显示表
       sql = "show tables" ;
        log.debug("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
          log.debug(res.getString(1));
        }
        //sql = "select * from tas_app_age_gprs_20140905" ;
        sql = "select STATIS_MONTH,BUSI_ID,KEY_WORD,SECTION_ID,BRAND_ID,WEB_ADDRESS from tas_app_age_gprs_20140905" ;
        log.debug("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
          //log.debug(res.getString(1)+"1\t"+res.getString(2)+"2\t"+res.getString(3)+"3\t"+res.getString(4)+"4\t"+res.getString(5)+"5\t"+res.getString(6));
            log.debug(res.getString(1)+"\t"+res.getString(2)+"\t"+res.getString(3)+"\t"+res.getString(4)+"\t"+res.getString(5)+"\t"+res.getString(6));
        }*/
        //con.commit();
       // res.close();
        stmt.close();
        con.close();
    }
}
