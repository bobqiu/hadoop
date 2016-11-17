package com.qiu.javabasic.disruptor.basic;

import com.lmax.disruptor.dsl.Disruptor;

import java.util.concurrent.CountDownLatch;

/**
 * Created by bob on 2016/11/17.
 */
public class TradeTransactionPublisher implements Runnable {
    Disruptor<TradeTransaction> disruptor;
    private CountDownLatch latch;
    private static int LOOP = 10000000;

    public TradeTransactionPublisher(Disruptor<TradeTransaction> disruptor, CountDownLatch latch) {
        this.disruptor = disruptor;
        this.latch = latch;
    }

    @Override

    public void run() {
        TradeTransactionEventTranslator translator = new TradeTransactionEventTranslator();
        for (int i = 0; i < LOOP; i++) {
            disruptor.publishEvent(translator);

        }
        latch.countDown();
    }
}
