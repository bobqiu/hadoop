package com.qiu.mptest.smallfileproc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.TestInputPath;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/7/15.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class SequenceWriter {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String outputPath = "hdfs://DamHadoop1:9000//user/hadoop/SequenceReadout/test.seq";
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(outputPath),conf);
            SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, conf, new Path(outputPath),Text.class, Text.class, SequenceFile.CompressionType.BLOCK, new BZip2Codec());
            writer.append(new Text("name"), new Text("http://www.369bi.com"));
            IOUtils.closeStream(writer);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
