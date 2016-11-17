package com.qiu.javabasic.disruptor.basic;

import com.lmax.disruptor.*;

import java.util.concurrent.*;

/**
 * Created by bob on 2016/11/17.
 */
public class TradeTrasactionProcess {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        int BUFFER_SIZE = 1024;
        int Thread_NUMBERS = 4;
        /**
         *RingBuffer是存储消息的地方，通过cursor的Sequence对象指示队列的头，协调RingBuffer中消息的添加，在消费端可以判断RingBuffer是否为空。
         * 表示队尾的Sequence并不在RingBuffer中，由消费者维护。通过gatingSequences的Sequence数组跟踪相关Sequene.
         */
        final RingBuffer<TradeTransaction> ringBuffer = RingBuffer.createSingleProducer(new EventFactory<TradeTransaction>() {


            public TradeTransaction newInstance() {
                return new TradeTransaction();
            }
        }, BUFFER_SIZE, new YieldingWaitStrategy());

        ExecutorService executorService = Executors.newFixedThreadPool(Thread_NUMBERS);

        /**
         * SequenceBarrier用来在消费都之间以及消费者和RingBuffer之间建立依赖关系。
         */
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
/**
 * waitStrtegy 等待策略，不同的等待策略在延迟和CPU资源的占用上有所不同
 * BusySpinWaitStrategy ： 自旋等待，类似Linux Kernel使用的自旋锁。低延迟但同时对CPU资源的占用也多。

 BlockingWaitStrategy ： 使用锁和条件变量。CPU资源的占用少，延迟大。

 SleepingWaitStrategy ： 在多次循环尝试不成功后，选择让出CPU，等待下次调度，多次调度后仍不成功，尝试前睡眠一个纳秒级别的时间再尝试。这种策略平衡了延迟和CPU资源占用，但延迟不均匀。

 YieldingWaitStrategy ： 在多次循环尝试不成功后，选择让出CPU，等待下次调。平衡了延迟和CPU资源占用，但延迟也比较均匀。

 PhasedBackoffWaitStrategy ： 上面多种策略的综合，CPU资源的占用少，延迟大。

 BatchEventProcessor
 在disruptor中，消费者是以EventProcessor的形式存在，其中一类消费者是BatchProcessor。都有一个Sequenece,来记录自己消费RingBuffer中消息的情况。
 WorkProcessor
 另一类消费者是WorkProcessor.每个WorkProcessor也有一个Sequene,多个workProcessor共享一个Sequence用于互斥访问的RingBuffer.
 */

        BatchEventProcessor<TradeTransaction> transProcessor = new BatchEventProcessor<TradeTransaction>(ringBuffer, sequenceBarrier, new TradeTransactionInDbHandler());

        ringBuffer.addGatingSequences(transProcessor.getSequence());

        executorService.submit(transProcessor);

        Future<?> future = executorService.submit(new Callable<Void>() {

            public Void call() throws Exception {
                long seq;
                for (int i = 0; i < 1000; i++) {
                    seq = ringBuffer.next();
                    ringBuffer.get(seq).setPrice(Math.random() * 9999);

                    ringBuffer.publish(seq);
                }
                return null;
            }
        });
        future.get();
        Thread.sleep(1000);
        transProcessor.halt();
        executorService.shutdown();

    }
}
