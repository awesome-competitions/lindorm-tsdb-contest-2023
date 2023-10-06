package com.alibaba.lindorm.contest.v2.tests;

import com.alibaba.lindorm.contest.v2.Const;

import java.util.HashSet;
import java.util.Set;

public class TestRange {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        int batch = 7_0000;
        for (int i = 0; i < batch; i++) {
            range();
        }
        System.out.println(System.currentTimeMillis() - start);
    }

    static long[] positions = new long[Const.TIME_SPAN];

    static long[] filter = new long[Const.TIME_SPAN];
    static void range(){
        for (int i = 0; i < Const.TIME_SPAN; i++) {
            filter[i] = 0;
        }
        long sum = 0;
        for (int i = 0; i < Const.TIME_SPAN; i++) {
            long pos = positions[i];
            if (filter[i] == 1){
                continue;
            }
            sum += pos;
            filter[i] = 1;
        }
    }
}
