package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.util.Column;
import com.alibaba.lindorm.contest.v2.codec.*;
import com.alibaba.lindorm.contest.v2.codec.Codec;

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

    // block size
    int BLOCK_SIZE = 200;
    // the time span of all the data
    int TIME_SPAN = 60 * 60 * 10;

    // column
    int COLUMN_COUNT = 60;
    List<String> COLUMNS = new ArrayList<>(COLUMN_COUNT);
    Map<String, Column> COLUMNS_INDEX = new HashMap<>(COLUMN_COUNT);

    // coding
    Map<String, Codec<int[]>> COLUMNS_CODEC = new HashMap<>(COLUMN_COUNT){{
        // Integer
        put("GMVS", Codec.runLengthIntCodec(15));
        put("LIOS", Codec.deltaOfDeltaIntCodec(3));
        put("LFZG", Codec.deltaOfDeltaIntCodec(3));
        put("WLYQ", Codec.deltaOfDeltaIntCodec(3));
        put("ZTGP", Codec.deltaOfDeltaIntCodec(3));
        put("GMNR", Codec.deltaOfDeltaIntCodec(3));
        put("BTJH", Codec.deltaOfDeltaIntCodec(3));
        put("XCDQ", Codec.deltaOfDeltaIntCodec(3));
        put("JKZL", Codec.deltaOfDeltaIntCodec(3));
        put("DSKQ", Codec.deltaOfDeltaIntCodec(3));
        put("MLBY", Codec.deltaOfDeltaIntCodec(3));
        put("EQLU", Codec.deltaOfDeltaIntCodec(3));
        put("KACE", Codec.deltaOfDeltaIntCodec(3));
        put("YDXB", Codec.deltaOfDeltaIntCodec(3));
        put("NXCR", Codec.deltaOfDeltaIntCodec(3));
        put("OZSM", Codec.deltaOfDeltaIntCodec(7));
        put("SYMN", Codec.deltaOfDeltaIntCodec(3));
        put("UAPJ", Codec.deltaOfDeltaIntCodec(3));
        put("PQRQ", Codec.deltaOfDeltaIntCodec(7));
        put("UICP", Codec.deltaOfDeltaIntCodec(7));
        put("KFGP", Codec.varIntCodec());
        put("ZZBE", Codec.varIntCodec());
        put("HJPZ", Codec.varIntCodec());
        put("JDOE", Codec.varIntCodec());
        put("KWET", Codec.varIntCodec());
        put("WNHB", Codec.varIntCodec());
        put("ENVH", Codec.varIntCodec());
        put("CSRC", Codec.varIntCodec());
        put("GONE", Codec.varIntCodec());
        put("BZPV", Codec.varIntCodec());
        put("AOAO", Codec.deltaIntCodec(3));
        put("EXGV", Codec.deltaIntCodec(3));
        put("JCGU", Codec.deltaIntCodec(3));
        put("TFGW", Codec.deltaIntCodec(3));
        put("MUBC", Codec.deltaIntCodec(3));
        put("JHET", Codec.deltaIntCodec(3));
        put("WKUZ", Codec.runLengthIntCodec(15));
        put("YCTI", Codec.deltaIntCodec(7));
        put("BBPX", Codec.deltaIntCodec(15));
        put("HRXI", Codec.deltaIntCodec(15));
//        // Double
//        put("FWKW", new Codec());
//        put("BDPO", new Codec());
//        put("FQTH", new Codec());
//        put("SBXA", new Codec());
//        put("XRTP", new Codec());
//        put("ZIKG", new Codec());
//        put("LMLK", new Codec());
//        put("TEDW", new Codec());
//        put("UVGJ", new Codec());
//        put("LYLI", new Codec());
//        // String
//        put("FVXS", new Codec());
//        put("LIYD", new Codec());
//        put("ZEBY", new Codec());
//        put("UFPI", new Codec());
//        put("UZSV", new Codec());
//        put("FLLY", new Codec());
//        put("JUBK", new Codec());
//        put("ORNI", new Codec());
//        put("SCHU", new Codec());
//        put("GLNG", new Codec());
    }};

    Codec<int[]> DEFAULT_INT_CODEC = new DefaultIntCodec();


}
