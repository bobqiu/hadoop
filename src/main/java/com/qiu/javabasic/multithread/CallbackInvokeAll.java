package com.qiu.javabasic.multithread;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by Administrator on 2016/10/18.
 */
public class CallbackInvokeAll {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        List<MyCallable> callableList = new ArrayList<MyCallable>();
        for(int i=0;i<10;i++) {
            callableList.add(new MyCallable(i));
        }

        List<Future<Integer>> result = executor.invokeAll(callableList);

        for (Future<Integer> future : result) {
            System.out.println(future.get());
        }

        ScheduledExecutorService executor1 = (ScheduledExecutorService) Executors.newScheduledThreadPool(20);

        ScheduledFuture<Integer> scheduledFuture=executor1.schedule(new MyCallable(2), 10, TimeUnit.DAYS);

        scheduledFuture.get();

        executor1.shutdown();

    }
}
