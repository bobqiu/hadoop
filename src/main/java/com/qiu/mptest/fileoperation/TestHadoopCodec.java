
package com.qiu.mptest.fileoperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.net.URI;

public class TestHadoopCodec {

    /**
     * @param args
     * @throws Exception 
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException, Exception {
        // TODO Auto-generated method stub
        String inputFile = "";
        String outPutDir = "hdfs://DamHadoop1:9000/user/hadoop/codec/";
        //hadoop文件的配置信息
        Configuration conf=new Configuration();
        conf.set("hadoop.job.ugi", "hadoop,hadoop");
        
      //测试各种压缩格式的效率 
        //gzip 
        long gzipTime = zipFile(conf, inputFile, outPutDir, "org.apache.hadoop.io.compress.GzipCodec", "gz"); 
        //bzip2 
        long bzip2Time = zipFile(conf, inputFile, outPutDir, "org.apache.hadoop.io.compress.BZip2Codec", "bz2"); 
        //deflate 
        long deflateTime = zipFile(conf, inputFile, outPutDir, "org.apache.hadoop.io.compress.DefaultCodec", "deflate"); 
         
        System.out.println("被压缩的文件名为： "+inputFile); 
        System.out.println("使用gzip压缩，时间为： "+gzipTime+"毫秒!"); 
        System.out.println("使用bzip2压缩，时间为： "+bzip2Time+"毫秒!"); 
        System.out.println("使用deflate压缩，时间为： "+deflateTime+"毫秒!"); 
    }

    static long zipFile(Configuration conf, String inputFile, String outputFolder, String codecClassName,
            String suffixName) throws IOException, Exception {
        long startTime=System.currentTimeMillis();
        //创建本地文件输入流
        InputStream is=new BufferedInputStream(new FileInputStream(inputFile));
        //获取文件名
        String fileName=inputFile.substring(0,inputFile.indexOf("."));
        //构建输出文件名
        String outFile=outputFolder+fileName+"."+suffixName;
        
        //构建hadoop文件系统
        FileSystem fs=FileSystem.get(URI.create(outFile),conf);
        
        //创建一个编码解码器，通过反射机制根据传入的来动态生成实例
        CompressionCodec codec=(CompressionCodec) ReflectionUtils.newInstance(Class.forName(codecClassName), conf);
        
        //创建一个指向hdfs目录的压缩文件输出流
        OutputStream os=codec.createOutputStream(fs.create(new Path(outFile)));
        try {
            IOUtils.copyBytes(is, os, conf);
        } catch (Exception e) {
            is.close();
            os.close();
            // TODO: handle exception
        }
        long endTime=System.currentTimeMillis();
        
        return endTime-startTime;
    }
}
