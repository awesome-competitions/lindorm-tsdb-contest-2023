package com.alibaba.lindorm.contest.util;


import com.alibaba.lindorm.contest.v1.Const;
import com.alibaba.lindorm.contest.v1.Table;

public class Monitor {
    public static Runtime runtime = Runtime.getRuntime();
    public static String information(){
        return "heap max: " + runtime.maxMemory() / Const.M + ", heap used: " + (runtime.totalMemory() - runtime.freeMemory()) / Const.M;
    }

    public static void start(){
        new Thread(()->{
            for (;;){
                try {
                    Thread.sleep(1000 * 5);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(information());
            }
        }).start();
    }
}