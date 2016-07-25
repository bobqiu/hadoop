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
public class MapFileWriter {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String outputFile="hdfs://DamHadoop1:9000//user/hadoop/mapfilewriteout";
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(outputFile),conf);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        Path path = new Path(outputFile+"/testMapfile");
        try {
            MapFile.Writer writer = new MapFile.Writer(conf, fileSystem, path.toString(), Text.class, Text.class);
            writer.append(new Text("测试"), new Text("http://www.369bi.com"));
            IOUtils.closeStream(writer);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
