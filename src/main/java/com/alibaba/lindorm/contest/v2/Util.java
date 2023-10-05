package com.alibaba.lindorm.contest.v2;

public class Util {

    public static long parseTimestampKey(long timestamp){
        return timestamp - timestamp % (2 * 60 * 1000);
    }

    public static void main(String[] args) {
        System.out.println(parseTimestampKey(System.currentTimeMillis()));
    }
}
