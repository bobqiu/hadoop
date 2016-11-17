package com.qiu.javabasic.disruptor.basic;


import com.lmax.disruptor.EventHandler;

/**
 * Created by bob on 2016/11/17.
 */
public class TradeTransactionJmsNotifyHandler implements EventHandler<TradeTransaction> {

    @Override
    public void onEvent(TradeTransaction event, long sequence, boolean endOfBatch) throws Exception {
        System.out.println("notify");

    }
}
