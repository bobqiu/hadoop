package com.qiu.javabasic.disruptor.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * Created by bob on 2016/11/17.
 */
public class LongEventFactory implements EventFactory<LongEvent> {
    public LongEvent newInstance() {
        return new LongEvent();
    }

}
