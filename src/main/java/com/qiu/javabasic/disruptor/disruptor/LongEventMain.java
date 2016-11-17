package com.qiu.javabasic.disruptor.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.jboss.netty.util.internal.ExecutorUtil;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by bob on 2016/11/17.
 */
public class LongEventMain {
    public static void main(String[] args) throws InterruptedException {
        LongEventFactory factory = new LongEventFactory();

        int bufferSize = 1024;

        Disruptor disruptor = new Disruptor<LongEvent>(factory, bufferSize, Executors.defaultThreadFactory(), ProducerType.SINGLE, new YieldingWaitStrategy());

        disruptor.handleEventsWith(new LongEventHandler());

        disruptor.start();

        RingBuffer<LongEvent> ringBuffer = disruptor.getRingBuffer();

        LongEventProducer producer = new LongEventProducer(ringBuffer);

        ByteBuffer byteBuffer = ByteBuffer.allocate(9);

        for (long i = 0; i < 100000; i++) {
            byteBuffer.putLong(0, i);
            producer.onData(byteBuffer);
            Thread.sleep(1000);
        }
        disruptor.shutdown();

        //ExecutorsUtils.shutdownAndAwaitTermination(executorService, 60, TimeUnit.SECONDS);
    }
}
