package com.qiu.javabasic.disruptor.basic;

import com.lmax.disruptor.EventFactory;

/**
 * Created by bob on 2016/11/17.
 */
public class TradeTrasactionFactory implements EventFactory<TradeTransaction> {
    public TradeTransaction newInstance() {

        return new TradeTransaction();
    }
}
