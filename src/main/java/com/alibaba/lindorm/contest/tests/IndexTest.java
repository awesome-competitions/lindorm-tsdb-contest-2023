package com.alibaba.lindorm.contest.tests;

import com.alibaba.lindorm.contest.impl.Index;

public class IndexTest {

    public static void main(String[] args) {

        Index index = new Index();
        index.put(1694078716000L, 1);
        index.put(1694078717000L, 1);
        index.put(1694078776000L, 1);
        index.put(1694078777000L, 1);

        index.forRangeEach(1694078716012L, 1694078776012L, (t, p) -> {
            System.out.println(t);
        });
    }
}
