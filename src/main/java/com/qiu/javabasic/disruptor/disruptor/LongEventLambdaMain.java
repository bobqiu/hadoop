package com.qiu.javabasic.disruptor.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

/**
 * Created by bob on 2016/11/17.
 */
public class LongEventLambdaMain {
    public static void main(String[] args) throws Exception {

        // 指明RingBuffer的大小，必须为2的幂
        int bufferSize = 1024;

        // 实例化Disruptor
        Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent::new,
                bufferSize, Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new YieldingWaitStrategy());

        // 置入处理逻辑
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> System.out.println("Event: " + event));

        disruptor.start();

        // 获取ringBuffer，用于发布事件
        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        LongEventProducer producer = new LongEventProducer(ringBuffer);

        ByteBuffer bb = ByteBuffer.allocate(8);
        for (long l = 0; l < 1000; l++) {
            bb.putLong(0, l);
            ringBuffer.publishEvent((event, sequence, buffer) -> event.set(buffer.getLong(0)), bb);
            Thread.sleep(1000);
        }
        disruptor.shutdown();
    }
}
