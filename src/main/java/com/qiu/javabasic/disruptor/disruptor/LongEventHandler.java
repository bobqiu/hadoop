package com.qiu.javabasic.disruptor.disruptor;

import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

/**
 * Created by bob on 2016/11/17.
 */
public class LongEventHandler implements com.lmax.disruptor.EventHandler {
    public void onEvent(Object event, long sequence, boolean endOfBatch) throws Exception {

        System.out.println("Event:" + event);

    }
}
