package com.qiu.mptest.cfm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/4.
 */
public class SumMapper extends Mapper<LongWritable, Text, IntWritable, TrackStats> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(" ");
        int trackId = Integer.parseInt(parts[1]);
        int scrobbles = Integer.parseInt(parts[2]);
        int radio = Integer.parseInt(parts[3]);
        int skip = Integer.parseInt(parts[4]);
        TrackStats trackStats = new TrackStats(0, scrobbles + radio, scrobbles, radio, skip);
        System.out.println("trackStats:" + trackStats);
        context.write(new IntWritable(trackId), trackStats);
    }
}
