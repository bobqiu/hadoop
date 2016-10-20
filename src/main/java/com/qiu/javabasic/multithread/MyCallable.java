package com.qiu.javabasic.multithread;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by Administrator on 2016/10/18.
 */
public class MyCallable implements Callable<Integer> {

    private int num;
    public MyCallable(int num) {
        this.num = num;
    }

    public Integer call() throws Exception {
        System.out.println(Thread.currentThread().getName()+"::");
        return num * 2;
    }
}
