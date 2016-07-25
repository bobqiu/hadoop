package com.qiu.mptest.smallfileproc;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Created by Administrator on 2016/7/12.
 */
public class CombinUseFs {
    public static void main(String[] args) throws  URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");
        FSDataOutputStream fsDataOutputStream = null;
        InputStream inputStream = null;
        InputStreamReader isr = null;
        String inputPath = "hdfs://DamHadoop1:9000//user/hadoop/linktest";
        String outPath="hdfs://DamHadoop1:9000//user/hadoop/";

        Path path = new Path(outPath + "/combinedfile");
        try {
            fsDataOutputStream = FileSystem.get(path.toUri(), conf).create(path);

            FileSystem fs = FileSystem.get(new URI(outPath), conf);

            FileStatus status[] = fs.listStatus(new Path(inputPath));
            for (int i = 0; i < status.length; i++) {
                String filePath = status[i].getPath().toString();
                System.out.println("文件路径名称：" + status[i].getPath().toString());
                Path inPath = new Path(filePath);

                inputStream = fs.open(inPath);

                isr = new InputStreamReader(inputStream, "utf8");

                List<String> readLines = IOUtils.readLines(isr);

                for (String line : readLines) {
                    fsDataOutputStream.write(line.getBytes());
                    fsDataOutputStream.write("\n".getBytes());

                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
                fsDataOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

    }

}
