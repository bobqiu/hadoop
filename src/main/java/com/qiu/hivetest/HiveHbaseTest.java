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
public class HiveHbaseTest {

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://10.1.251.122:10000/hivetest", "hadoop", "hadoop");
        Statement stmt = con.createStatement();
        
        //创建hive能识别的表，并创建与hbase的关系　:key 默认是statis_month的值
        StringBuilder sb = new StringBuilder(" CREATE TABLE hive_tas_app_age_gprs_20140903(STATIS_MONTH STRING,BUSI_ID STRING,KEY_WORD STRING,SECTION_ID STRING,BRAND_ID STRING,WEB_ADDRESS STRING)              \n");
        sb.append(" STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'  \n");
        sb.append(" WITH SERDEPROPERTIES (\"hbase.columns.mapping\"=\":key,cf1:BUSIID,cf1:KEYWORD,cf1:SECTIONID,cf1:BRANDID,cf1:WEBADDRESS\")   \n");
        sb.append(" TBLPROPERTIES (\"hbase.table.name\" =\"hbase_tas_app_age_gprs_20140903\")                     \n");
        System.out.println("执行创建表开始：SQL="+sb.toString());
        stmt.execute(sb.toString());
        System.out.println("执行创建表结束：SQL="+sb.toString());
        
        //导入数据 tas_app_age_gprs_20140905中key值不能重复
        sb = new StringBuilder("insert overwrite table hive_tas_app_age_gprs_20140903 select STATIS_MONTH,BUSI_ID,KEY_WORD,SECTION_ID,BRAND_ID,WEB_ADDRESS from tas_app_age_gprs_20140905");
        System.out.println("执行导入数据开始：sql="+sb.toString());
        stmt.execute(sb.toString());
        System.out.println("执行导入数据完成");
        
        //查询hive中的数据
        String sql = "select STATIS_MONTH,BUSI_ID,KEY_WORD,SECTION_ID,BRAND_ID,WEB_ADDRESS from hive_tas_app_age_gprs_20140903" ;
        System.out.println("执行查询：Running　sql: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
          //System.out.println(res.getString(1)+"1\t"+res.getString(2)+"2\t"+res.getString(3)+"3\t"+res.getString(4)+"4\t"+res.getString(5)+"5\t"+res.getString(6));
            System.out.println(res.getString(1)+"\t"+res.getString(2)+"\t"+res.getString(3)+"\t"+res.getString(4)+"\t"+res.getString(5)+"\t"+res.getString(6));
        }
        
        res.close();
        stmt.close();
        con.close();
    }
}
