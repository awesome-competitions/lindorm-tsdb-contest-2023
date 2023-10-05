package com.alibaba.lindorm.contest.v2;

public interface Const {

    // unit
    int K = 1024;
    int M = 1024 * K;
    int G = 1024 * M;
    int BYTE_BUFFER_SIZE = 512 * K;

    // block size
    int BLOCK_SIZE = 60 * 2;
    // the time span of all the data
    int TIME_SPAN = 60 * 60 * 10;

    int INDEX_POSITIONS_SIZE = TIME_SPAN / BLOCK_SIZE;



}
