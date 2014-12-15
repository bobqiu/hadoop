/*
 * @(#)FileOperation.java
 * 
 * CopyRight (c) 2014 保留所有权利。
 */

package com.qiu.mptest.fileoperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

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
            FileSystem fs = FileSystem.get(new Configuration());
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
    
    //对文件的内容输入并定位文本中的某一位置
    static void searchTextFromFile(){
        FileSystem fs;
        FSDataInputStream in = null;

        try {
            Configuration conf=new Configuration();
             fs = FileSystem.get(conf);
             in=fs.open(new Path("hdfs://DamHadoop1:9000/user/hadoop"));
            //将Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
            IOUtils.copyBytes(in, System.out, 50,false);
            System.out.println();
            //展示FSDataInputStream文件输入流的流定位能力，用seek进行定位
            //把文件输出3次，第一次输入全部内容，第二次输入从第20个字符开始的内容，第3次输出从第40个字符开始的内容
            for(int i=1;i<=3;i++){
                in.seek(0+20*(i-1));
                System.out.println("运行"+i+"次");
                IOUtils.copyBytes(in, System.out, 4096, false);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(in);
        }
       
    }
    //使用FSDataOutputStream来写文件到hdfs系统中，从本地文件系统中复制文件到hdfs文件系统中
    //使用IOUtils.copy(),复制文件另一种方式fs.copyToLocalFile
    static void writeToHdfsFromLocal() throws IOException{
        String localSrc="";
        String destSrc="";
        //创建本地文件输入流
        InputStream in=new BufferedInputStream(new FileInputStream(localSrc));
        //读取hadoop的配置文件
        Configuration conf=new Configuration();
        conf.set("hadoop.job.ugi", "hadoop");
        //初始化FileSystem
        FileSystem fs=null;
        try {
            fs=FileSystem.get(URI.create(destSrc),conf);
            OutputStream out=fs.create(new Path(destSrc));
            //使用IOUtils工具复制本地文件到hdfs目标文件中
            IOUtils.copyBytes(in, out, 4098, true);
            System.out.println("OK!");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            fs.close();
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
      deleteFile();
        //在hdfs中创建文件
     //   createFileOnHDFS();
        //查询文件状态
      // SearchFileStatus();
        //读取hdfs指定目录下的文件，并显示内容
       // readHdfsAllFiles();
        //从hdfs上下载文件
        //downloadFileFromHdfs();
        //从本地上传文件到hdfs中
       // uploadFileFromLocal();
        //创建文件夹
        //createFileDir();

    }

}
