package com.qiu.javabasic.disruptor.basic;

import javafx.event.Event;

/**
 * Created by bob on 2016/11/17.
 */
public class TradeTransaction extends Event {
    private String id; //交易Id
    private double price; //交易金额

    public TradeTransaction() {

    }

    public TradeTransaction(String id, double price) {

        this.id = id;
        this.price = price;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
