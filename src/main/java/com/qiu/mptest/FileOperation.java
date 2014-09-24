/*
 * @(#)FileOperation.java
 * 
 * CopyRight (c) 2014 保留所有权利。
 */

package com.qiu.mptest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * Title : FileOperation
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
 * 1    2014-9-18    qiubo        Created
 * </pre>
 * <p/>
 * 
 * @author qiubo
 * @version 1.0.0.2014-9-18
 */
public class FileOperation {
    //从hdfs中下载文件或文件夹到本地
    static void downloadFileFromHdfs(){
        Configuration conf=new Configuration();
        try {
            FileSystem fs=FileSystem.get(conf);
            Path p1=new Path("hdfs://DamHadoop1:9000/user/hadoop/data.txt");
            Path p2=new Path("E://");
            fs.copyToLocalFile(p1, p2);
            System.out.println("文件下载成功："+p1.toString()+"->"+p2.toString());
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    //从本地上传文件到hdfs中
    static void uploadFileFromLocal(){
        Configuration conf=new Configuration();
        try {
            FileSystem fs=FileSystem.get(conf);
            Path src=new Path("E://CreateSql.txt");
            Path dst=new Path("hdfs://DamHadoop1:9000/user/hadoop/mkdirTest");
            fs.copyFromLocalFile(src, dst);
            System.out.println("上传文件成功："+src+"->"+dst);
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    //删除文件
    static void deleteFile() {
        String hdfsUrl="hdfs://DamHadoop1:9000/user/hadoop";
        Path path=new Path("/abc.txt");
        Configuration conf=new Configuration();//获取配置信息
        try {
            //FileSystem fs=DistributedFileSystem.get(URI.create(hdfsUrl), conf);这种是错误的方法
            FileSystem fs=FileSystem.get(conf);
            path=new Path("hdfs://DamHadoop1:9000/user/hadoop/test2.txt");
            System.out.println(fs.delete(path,true));
            System.out.println("删除文件成功："+hdfsUrl+path);
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }
    //读取根目录下的二级目录中所有文件并打印出文件内容
    static void readHdfsAllFiles(){
        //初始化配置信息
        Configuration conf=new Configuration();
        try {
            //通过配置信息获取hdfs上的FileSystem
            FileSystem fs=FileSystem.get(conf);
            InputStream in =null;
            //使用缓冲流，进行按行读取的功能
            BufferedReader buff=null;
            //获取根路径
            //Path path=new Path("hdfs://DamHadoop1:9000/hive/warehouse");
            Path path=new Path("hdfs://DamHadoop1:9000/user/hadoop");
            //获取根目录下的所有子文件目录
            FileStatus stats[]=fs.listStatus(path);
            for(int i=0;i<stats.length;i++){
                //获取子目录下的文件路径
                FileStatus temp[]=fs.listStatus(new Path(stats[i].getPath().toString()));
                for(int k=0;k<temp.length;k++){
                    System.out.println("文件路径名："+temp[k].getPath().toString());
                    //获取最底层目录path
                    Path p=new Path(temp[k].getPath().toString());
                    //打开文件流
                    in=fs.open(p);
                    //包装成一个流
                    buff=new BufferedReader(new InputStreamReader(in));
                    String str=null;
                    while ((str=buff.readLine())!=null) {
                        System.out.println(str);
                    }
                    buff.close();
                    in.close();
                }
            }
            fs.close();
            
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
    }
    //在HDFS上创建一个文件夹
    static void createFileDir(){
        Configuration conf=new Configuration();
        try {
            FileSystem fs=FileSystem.get(conf);
            Path path=new Path("hdfs://DamHadoop1:9000/user/hadoop/mkdirTest");
            fs.mkdirs(path);
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    /** 
     * 在HDFS上创建一个文件 
     *  
     * **/  
    static void createFileOnHDFS(){  
          
           FileSystem fs;
        try {
            fs = FileSystem.get(new Configuration());
            Path p =new Path("hdfs://DamHadoop1:9000/user/hadoop/abc.txt");  
            fs.createNewFile(p);  
            //fs.create(p);  
            fs.close();//释放资源  
            System.out.println("创建文件成功.....");  
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }  
    }  
    //重命名文件夹
    static void renameFile(){
        try {
            FileSystem fs =FileSystem.get(new Configuration());
            Path p1=new Path("hdfs://DamHadoop1:9000/user/hadoop/test2.txt");
            Path p2=new Path("hdfs://DamHadoop1:9000/user/hadoop/test1.txt");
            fs.rename(p1, p2);
            fs.close();
            System.out.println("重命名文件夹成功");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
 
    //查询文件或目录状态
    static void SearchFileStatus() {
        String dfsLocal = "hdfs://DamHadoop1:9000/user/hadoop";
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(dfsLocal), conf);
            // fs.mkdirs(new Path(""));
            // fs.delete(new Path("/user"));
            System.out.println(fs.exists(new Path("/user")));
            // 获取文件目录路径
            FileStatus[] fileStatus = fs.listStatus(new Path("/user"));
            for (FileStatus fileStatus2 : fileStatus) {
                System.out.println("目录blockSize:" + fileStatus2.getBlockSize());
            }
            System.out.println();
            Path[] listpaths = FileUtil.stat2Paths(fileStatus);
            for (Path p : listpaths) {
                System.out.println(p);// 返回hdfs://DamHadoop1:9000/user/hadoop
            }
            // 获取某个文件的元信息
            FileStatus fstatus = fs.getFileStatus(new Path("/user/hadoop/data.txt"));
            System.out.println("文件路径：" + fstatus.getPath());
            System.out.println("文件长度：" + fstatus.getLen());
            fs.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Description:
     * 
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        //重命名文件
      // renameFile1();
        //删除文件
      // deleteFile();
        //在hdfs中创建文件
     //   createFileOnHDFS();
        //查询文件状态
       // SearchFileStatus();
        //读取hdfs指定目录下的文件，并显示内容
       // readHdfsAllFiles();
        //从hdfs上下载文件
        //downloadFileFromHdfs();
        //从本地上传文件到hdfs中
        uploadFileFromLocal();
        //创建文件夹
        //createFileDir();

    }

}
