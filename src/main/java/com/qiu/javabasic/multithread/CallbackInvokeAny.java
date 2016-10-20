package com.qiu.javabasic.multithread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Administrator on 2016/10/18.
 */
public class CallbackInvokeAny {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        List<MyCallable> callableList = new ArrayList<MyCallable>();

        for(int i=0;i<10;i++) {
            callableList.add(new MyCallable(i));
        }
        Integer result = executor.invokeAny(callableList);
        System.out.println(result);

    }

}
