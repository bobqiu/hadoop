package com.qiu.javabasic.multithread;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Administrator on 2016/10/18.
 */
public class CallbackTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        MyCallable myCallable = new MyCallable(3);

        while (true) {
            Future<Integer> result = executor.submit(myCallable);
            System.out.println(result.get());
        }



    }
}
