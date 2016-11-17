package com.qiu.javabasic.disruptor.basic;


import com.lmax.disruptor.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by bob on 2016/11/17.
 */
public class TradeTrasactionProcessByWorkerPool {
    public static void main(String[] args) throws InterruptedException {
        int buffer_size = 1024;

        int thread_number = 4;

        EventFactory<TradeTransaction> eventFactory = new EventFactory<TradeTransaction>() {
            @Override
            public TradeTransaction newInstance() {
                return new TradeTransaction();
            }

        };

        RingBuffer<TradeTransaction> ringBuffer = RingBuffer.createSingleProducer(eventFactory, buffer_size);

        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();

        ExecutorService executorService = Executors.newFixedThreadPool(thread_number);

        WorkHandler<TradeTransaction> workHandler = new TradeTransactionInDbHandler();

        WorkerPool<TradeTransaction> workerPool = new WorkerPool<TradeTransaction>(ringBuffer, sequenceBarrier, new IgnoreExceptionHandler(), workHandler);

        workerPool.start(executorService);

        for (int i = 0; i < 100000; i++) {
            long seq = ringBuffer.next();
            ringBuffer.get(seq).setPrice(Math.random() * 9999);
            ringBuffer.publish(seq);

        }
        Thread.sleep(1000);
        workerPool.halt();
        executorService.shutdown();
    }


}
