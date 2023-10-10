package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.Row;

import java.util.*;

public interface Const {

    // settings
    String TEST_DATA_DIR = "D:\\Workspace\\tests\\test-tsdb\\";
    ArrayList<Row> EMPTY_ROWS = new ArrayList<>();
    Set<String> EMPTY_COLUMNS = new HashSet<>();
    int DATA_FILE_COUNT = 4;

    int K = 1024;
    int M = 1024 * K;
    int G = 1024 * M;
    int BYTE_BUFFER_SIZE = 512 * K;
    int DATA_BUFFER_SIZE = 16 * M;

    // vin
    int VIN_COUNT = 5000;

    // column
    int COLUMN_COUNT = 60;
    List<String> SORTED_COLUMNS = new ArrayList<>(Const.COLUMN_COUNT);
    Map<String, Colum> COLUMNS_INDEX = new java.util.HashMap<>(Const.COLUMN_COUNT);

    // block size
    int BLOCK_SIZE = 20;
    // the time span of all the data
    int TIME_SPAN = 60 * 60 * 10;

    int INDEX_POSITIONS_SIZE = TIME_SPAN / BLOCK_SIZE;

}
