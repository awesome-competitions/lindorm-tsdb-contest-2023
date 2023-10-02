package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.Row;

import java.util.ArrayList;

public interface Const {

    String TEST_DATA_DIR = "/Users/nico/workspace/nico/test/lindorm-tsdb/";

    int BITS = 8;

    int K = 1024;
    int M = 1024 * K;
    int G = 1024 * M;

    int MAX_ROW_SIZE = ((4 + 17 + 6 + 60 * 1024) / (4 * K) + 1) * (4 * K);

    int INITIALIZE_VIN_POSITIONS_SIZE = 10 * 3600;

    int MAX_VIN_COUNT = 5000;


    int TIMESTAMP_BITS = Util.calculateBits(Util.expressTimestamp(1689094801000L), true);
    int INT_BYTES_BITS = 6;
    int DOUBLE_DECIMALS = 10;
    int DOUBLE_EXPAND_MULTIPLE = (int) Math.pow(10, DOUBLE_DECIMALS);

    int ROW_LEN_BYTES = 2;
    int VIN_ID_BITS = 32;

    long BEGIN_TIMESTAMP = 1689091199000L;

    String VIN_PREFIX = "LSVNV2182E";

    ArrayList<Row> EMPTY_ROWS = new ArrayList<>();

}
