package com.qiu.javabasic.disruptor.disruptor;

import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;

/**
 * Created by bob on 2016/11/17.
 */
public class LongEventProducer {

    private RingBuffer<LongEvent> ringBuffer;

    public LongEventProducer(RingBuffer<LongEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(ByteBuffer bufferSize) {
        long sequence = ringBuffer.next();
        try {

            LongEvent event = ringBuffer.get(sequence);
            event.set(bufferSize.getLong(0));
        } finally {
            ringBuffer.publish(sequence);

        }
    }
}
