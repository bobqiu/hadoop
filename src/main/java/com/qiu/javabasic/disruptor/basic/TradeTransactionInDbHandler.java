package com.qiu.javabasic.disruptor.basic;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WorkHandler;

import java.util.UUID;


/**
 * Created by bob on 2016/11/17.
 */
public class TradeTransactionInDbHandler implements EventHandler<TradeTransaction>, WorkHandler<TradeTransaction> {

    public void onEvent(TradeTransaction event) throws Exception {
        event.setId(UUID.randomUUID().toString());
        System.out.println(event.getId());

    }

    public void onEvent(TradeTransaction event, long sequence, boolean endOfBatch) throws Exception {
        this.onEvent(event);

    }
}
