package com.alibaba.lindorm.contest.util;


import com.alibaba.lindorm.contest.v2.Const;
import com.alibaba.lindorm.contest.v2.Table;

public class Monitor {
    public static Runtime runtime = Runtime.getRuntime();
    public static String information(Table table){
        return "heap max: " + runtime.maxMemory() / Const.M + ", heap used: " + (runtime.totalMemory() - runtime.freeMemory()) / Const.M + ", index size: " + table.getIndexes().size();
    }

    public static String simpleInformation(){
        return runtime.maxMemory() / Const.M + "," + (runtime.totalMemory() - runtime.freeMemory()) / Const.M;
    }

    public static void start(Table table){
        new Thread(()->{
            for (;;){
                try {
                    Thread.sleep(1000 * 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(information(table));
            }
        }).start();
    }

    public static void startSimple(){
        new Thread(()->{
            for (;;){
                try {
                    Thread.sleep(1000 * 10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.print(simpleInformation() + ";");
            }
        }).start();
    }
}