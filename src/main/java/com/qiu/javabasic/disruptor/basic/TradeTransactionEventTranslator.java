package com.qiu.javabasic.disruptor.basic;

import com.lmax.disruptor.EventTranslator;

import java.util.Random;

/**
 * Created by bob on 2016/11/17.
 */
public class TradeTransactionEventTranslator implements EventTranslator<TradeTransaction> {
    private Random random = new Random();

    public TradeTransactionEventTranslator() {
    }


    public void translateTo(TradeTransaction event, long sequence) {
        this.generateTradeTransaction(event);
    }

    private TradeTransaction generateTradeTransaction(TradeTransaction event) {
        event.setPrice(random.nextDouble() * 9999);
        return event;
    }
}
