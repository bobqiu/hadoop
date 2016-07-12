package com.qiu.mptest.cfm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Administrator on 2016/7/4.
 */
public class UniqueListenerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private  final static Log log= LogFactory.getLog(UniqueListenerReducer.class);
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        List<Integer> userIds = new ArrayList<Integer>();
       // System.out.println("reduce1 ::key:value="+key+":"+values);
       // log.debug("reduce1 ::key:value="+key+":"+values);
        for (IntWritable value : values) {
            int userid = value.get();
            System.out.println("key+userid::"+key.toString()+":"+userid);
            /*log.debug("userid::"+userid);*/
            userIds.add(userid);
            System.out.println("userid size:"+userIds.size());
        }
        //System.out.println("reduce2 ::key:value="+key+":"+values);
        context.write(key, new IntWritable(userIds.size()));
    }
}
