package com.qiu.mptest.MapFileWriteRead;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/7/15.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class MapFileReader {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String inputFile = "hdfs://DamHadoop1:9000//user/hadoop/mapfilewriteout";
        try {
            FileSystem fileSystem = FileSystem.get(new URI(inputFile), conf);
            Path path = new Path(inputFile + "/testMapfile");

            MapFile.Reader reader = new MapFile.Reader(fileSystem, path.toString(), conf);

            Text key = new Text();
            Text value = new Text();

            while (reader.next(key, value)) {
                System.out.println("key="+key+"::value="+value.toString());
            }
            IOUtils.closeStream(reader);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
