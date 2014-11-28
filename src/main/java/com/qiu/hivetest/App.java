package com.qiu.hivetest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *　远程连接hive库，并做简单的show table,describe table,及简单的查询
 *  如果使用select * 则不会将查询提交到hadoop平台，但是使用具体字段查询如select name 则会将查询提交到hadoop平台执行。
 *
 */
public class App 
{
    public static void main( String[] args ) throws ClassNotFoundException
    {
        Class.forName("org.apache.hive.jdbc.HiveDriver");  
        try{
               Connection con = DriverManager.getConnection("jdbc:hive2://10.1.251.122:10000/default","hadoop","hadoop");  
               /*PreparedStatement sta = con.prepareStatement("select cast(date as date) from ccp group bycast(date as date)");  
               ResultSet result = sta.executeQuery();  
               while(result.next()){  
                      System.out.println(result.getDate(1));  
               }  */
               //CREATE DATABASE financials;　CREATE DATABASE IF NOT EXISTS financials;
               //hive> CREATE DATABASE financials
               // LOCATION '/my/preferred/directory';
               // show tables
               Statement stmt = con.createStatement();
               String sql = "show tables" ;
               System.out.println("Running: " + sql);
               ResultSet res = stmt.executeQuery(sql);
               while (res.next()) {
                 System.out.println(res.getString(1));
               }
               // describe table
               sql = "describe " + res.getString(1);
               System.out.println("Running: " + sql);
               res = stmt.executeQuery(sql);
               while (res.next()) {
                 System.out.println(res.getString(1) + "\t" + res.getString(2));
               }
            // select  table
               sql = "select col1,col2,col3,col4 from hbaseive ";
               System.out.println("Running: " + sql);
               res = stmt.executeQuery(sql);
               while (res.next()) {
                 System.out.println(res.getString(1) + "\t" + res.getString(2));
               }
               res.close();
               stmt.close();
               con.close();
        } catch(SQLException e) {  
               e.printStackTrace();  
        }  
    }
}
