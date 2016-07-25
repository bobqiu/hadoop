package com.qiu.mptest.smallfileproc;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/7/15.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class SequenceReader {
    private static final String INPUTPATH = "hdfs://DamHadoop1:9000//user/hadoop/SequenceReadout";

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("HADOOP_USER_NAME","hadoop");
        try {
            FileSystem fs = FileSystem.get(new URI(INPUTPATH),conf);
            Path path = new Path(INPUTPATH + "/test.seq");
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

            Text key = new Text();
            Text value = new Text();

            while (reader.next(key, value)) {
                System.out.println("key=" + key + ":value" + value);
                System.out.println("position=" + reader.getPosition());
            }
            IOUtils.closeStream(reader);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }
}
