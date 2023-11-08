package com.alibaba.lindorm.contest.v2.tests;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;

public class TestCDL {

    public static void main(String[] args) throws InterruptedException {

        int batch = 1000000;

        Semaphore semaphore = new Semaphore(0);
        long s = System.currentTimeMillis();
        ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(16);
        for (int k = 0; k < batch; k++) {
            for (int i = 0; i < 10; i++) {
                poolExecutor.execute(semaphore::release);
            }
            semaphore.acquire(10);
        }

        System.out.println("cost:" + (System.currentTimeMillis() - s));


    }
}
