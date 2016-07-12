package com.qiu.mptest.cfm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/3.
 *0 11115 222 0 1 0
 *1 11113 225 1 0 0
 *2 11117 223 0 1 0
 *3 11115 225 1 0 0
 **
 */
public class UniqueListenerMapper extends Mapper<Object , Text, Text, IntWritable> {

    protected void map(Object  key, Text rawLine, Context context) throws IOException, InterruptedException {
        String[] parts = rawLine.toString().split(" ");
        System.out.println("map::key: value=" + parts[1] + ":" + parts[0]);
       /* int musicRecords = Integer.parseInt(parts[2]);
        int isListens = Integer.parseInt(parts[3]);
        if (musicRecords <= 0 && isListens <= 0) {
            return;
        }*/
        Text trackId = new Text(parts[2]);

        IntWritable userId = new IntWritable(Integer.parseInt(parts[1]));

        context.write(trackId, userId);
    }
}
