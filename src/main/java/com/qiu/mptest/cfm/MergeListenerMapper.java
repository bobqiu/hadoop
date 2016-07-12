package com.qiu.mptest.cfm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/5.
 */
public class MergeListenerMapper extends Mapper<IntWritable, IntWritable, IntWritable, TrackStats> {
    @Override
    protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
        TrackStats trackStats = new TrackStats();
        trackStats.setListeners(value.get());
        System.out.println("mergeListenerMaper:"+trackStats);
        context.write(key, trackStats);
    }
}
