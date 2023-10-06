package com.alibaba.lindorm.contest.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface Const {

    // unit
    int K = 1024;
    int M = 1024 * K;
    int G = 1024 * M;
    int BYTE_BUFFER_SIZE = 512 * K;

    // column
    int COLUMN_COUNT = 60;
    List<String> SORTED_COLUMNS = new ArrayList<>(Const.COLUMN_COUNT);
    Map<String, Colum> COLUMNS_INDEX = new java.util.HashMap<>(Const.COLUMN_COUNT);

    // block size
    int BLOCK_SIZE = 60 * 2;
    // the time span of all the data
    int TIME_SPAN = 60 * 60 * 10;

    int INDEX_POSITIONS_SIZE = TIME_SPAN / BLOCK_SIZE;


}
