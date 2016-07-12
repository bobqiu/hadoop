package com.qiu.mptest.cfm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/4.
 */
public class SumReducer extends Reducer<IntWritable, TrackStats, IntWritable, TrackStats> {
    @Override
    protected void reduce(IntWritable trackId, Iterable<TrackStats> values, Context context) throws IOException, InterruptedException {
        TrackStats sum = new TrackStats();
        for (TrackStats trackStats : values) {
            TrackStats trackStat = new TrackStats();
            System.out.println("reducer trackstats:" + trackStat);
            sum.setListeners(sum.getListeners() + trackStat.getListeners());
            sum.setPlays(sum.getPlays() + trackStat.getPlays());
            sum.setRadio(sum.getRadio() + trackStat.getRadio());
            sum.setSkip(sum.getSkip() + trackStat.getSkip());
            sum.setScrobbles(sum.getScrobbles() + trackStat.getScrobbles());
        }
        System.out.println("sum:"+sum);
        context.write(trackId, sum);
    }
}
