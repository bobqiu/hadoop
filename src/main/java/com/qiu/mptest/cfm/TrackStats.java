package com.qiu.mptest.cfm;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2016/7/4.
 */
public class TrackStats implements WritableComparable<TrackStats>
{
    IntWritable scrobbles;
    IntWritable radio;
    IntWritable skip;
    IntWritable plays;
    IntWritable listeners;

    public TrackStats() {
        this.scrobbles = new IntWritable();
        this.radio = new IntWritable();
        this.skip = new IntWritable();
        this.plays = new IntWritable();
        this.listeners = new IntWritable();
    }
    public TrackStats(int listeners,int plays,int scrobbles,int radio,int skip) {
        this.scrobbles = new IntWritable(scrobbles);
        this.radio = new IntWritable(radio);
        this.skip = new IntWritable(skip);
        this.plays = new IntWritable(plays);
        this.listeners = new IntWritable(listeners);
    }
    public int compareTo(TrackStats o) {
        return 0;
    }

    public void write(DataOutput out) throws IOException {
        listeners.write(out);
        scrobbles.write(out);
        plays.write(out);
        skip.write(out);
        radio.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        listeners.readFields(in);
        plays.readFields(in);
        scrobbles.readFields(in);
        radio.readFields(in);
        skip.readFields(in);
    }

    public int getRadio() {
        return radio.get();
    }

    public void setRadio(int radio) {
        this.radio.set(radio);
    }

    public int getSkip() {
        return skip.get();
    }

    public void setSkip(int skip) {
        this.skip.set(skip);
    }

    public int getPlays() {
        return plays.get();
    }

    public void setPlays(int plays) {
        this.plays.set(plays);
    }

    public int getListeners() {
        return listeners.get();
    }

    public void setListeners(int listeners) {
        this.listeners.set(listeners);
    }

    public int getScrobbles() {

        return scrobbles.get();
    }

    public void setScrobbles(int scrobbles) {
        this.scrobbles .set( scrobbles);
    }

    @Override
    public String toString() {
        return "TrackStats{" +
                "scrobbles=" + scrobbles +
                ", radio=" + radio +
                ", skip=" + skip +
                ", plays=" + plays +
                ", listeners=" + listeners +
                '}';
    }
}
