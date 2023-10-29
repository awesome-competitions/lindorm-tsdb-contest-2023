package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.util.Column;
import com.alibaba.lindorm.contest.v2.codec.*;
import com.alibaba.lindorm.contest.v2.codec.Codec;

import java.nio.ByteBuffer;
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
    int BLOCK_SIZE = 600;
    // the time span of all the data
    int TIME_SPAN = 60 * 60 * 10;

    int TIMESTAMP_INTERVAL = 1000;

    // column
    int COLUMN_COUNT = 60;
    int INT_COLUMN_COUNT = 40;
    int DOUBLE_COLUMN_COUNT = 10;
    int STRING_COLUMN_COUNT = 10;
    List<String> COLUMNS = new ArrayList<>(COLUMN_COUNT);
    List<String> INT_COLUMNS = new ArrayList<>(INT_COLUMN_COUNT);
    List<String> DOUBLE_COLUMNS = new ArrayList<>(DOUBLE_COLUMN_COUNT);
    List<String> STRING_COLUMNS = new ArrayList<>(STRING_COLUMN_COUNT);

    long[] COLUMNS_SIZE = new long[COLUMN_COUNT];

    Map<String, Column> COLUMNS_INDEX = new HashMap<>(COLUMN_COUNT);

    // coding
    Map<String, Codec<int[]>> COLUMNS_INTEGER_CODEC = new HashMap<>(COLUMN_COUNT){{
        // Integer
        put("GMVS", Codec.runLengthIntCodec(BLOCK_SIZE));
        put("LIOS", Codec.deltaOfDeltaIntCodec());
        put("LFZG", Codec.deltaOfDeltaIntCodec());
        put("WLYQ", Codec.deltaOfDeltaIntCodec());
        put("ZTGP", Codec.deltaOfDeltaIntCodec());
        put("DSKQ", Codec.deltaOfDeltaIntCodec());
        put("GMNR", Codec.deltaOfDeltaIntCodec());
        put("BTJH", Codec.deltaOfDeltaIntCodec());
        put("XCDQ", Codec.deltaOfDeltaIntCodec());
        put("JKZL", Codec.deltaOfDeltaIntCodec());
        put("MLBY", Codec.deltaOfDeltaIntCodec());
        put("EQLU", Codec.deltaOfDeltaIntCodec());
        put("KACE", Codec.deltaOfDeltaIntCodec());
        put("YDXB", Codec.deltaOfDeltaIntCodec());
        put("NXCR", Codec.deltaOfDeltaIntCodec());
        put("OZSM", Codec.deltaOfDeltaIntCodec());
        put("SYMN", Codec.deltaOfDeltaIntCodec());
        put("UAPJ", Codec.deltaOfDeltaIntCodec());
        put("PQRQ", Codec.deltaOfDeltaIntCodec());
        put("UICP", Codec.deltaOfDeltaIntCodec());
        put("KFGP", Codec.simpleNBCodec());
        put("ZZBE", Codec.simpleNBCodec());
        put("HJPZ", Codec.simpleNBCodec());
        put("JDOE", Codec.simpleNBCodec());
        put("KWET", Codec.simpleNBCodec());
        put("WNHB", Codec.simpleNBCodec());
        put("ENVH", Codec.simpleNBCodec());
        put("CSRC", Codec.simpleNBCodec());
        put("GONE", Codec.simpleNBCodec());
        put("BZPV", Codec.simpleNBCodec());
        put("AOAO", Codec.fixedIntCodec(-2147483647, -2147483645));
        put("EXGV", Codec.fixedIntCodec(-2147483647, -2147483645));
        put("JCGU", Codec.fixedIntCodec(-2147483647, -2147483645));
        put("TFGW", Codec.fixedIntCodec(-2147483647, -2147483645));
        put("MUBC", Codec.fixedIntCodec(-2147483647, -2147483645));
        put("JHET", Codec.fixedIntCodec(1, 3));
        put("WKUZ", Codec.runLengthIntCodec(BLOCK_SIZE));
        put("YCTI", Codec.fixedIntCodec(1, 5));
        put("BBPX", Codec.fixedIntCodec(1, 9));
        put("HRXI", Codec.fixedIntCodec(1, 9));
    }};

    Map<String, Codec<double[]>> COLUMNS_DOUBLE_CODEC = new HashMap<>(COLUMN_COUNT){{
        put("FWKW", Codec.deltaOfDeltaBDFCMCodec());
        put("BDPO", Codec.deltaOfDeltaBDFCMCodec());
        put("FQTH", Codec.deltaOfDeltaBDFCMCodec());
        put("SBXA", Codec.deltaOfDeltaBDFCMCodec());
        put("XRTP", Codec.deltaOfDeltaBDFCMCodec());
        put("ZIKG", Codec.bdfcmCodec());
        put("LMLK", Codec.bdfcmCodec());
        put("TEDW", Codec.bdfcmCodec());
        put("UVGJ", Codec.bdfcmCodec());
        put("LYLI", Codec.bdfcmCodec());
    }};

    Map<String, Codec<ByteBuffer[]>> COLUMNS_STRING_CODEC = new HashMap<>(COLUMN_COUNT){{
        put("FVXS", Codec.stringCodec());
        put("LIYD", Codec.stringCodec(1));
        put("ZEBY", Codec.stringCodec(7));
        put("UFPI", Codec.stringCodec(5));
        put("UZSV", Codec.stringCodec(15));
        put("FLLY", Codec.stringCodec(1));
        put("JUBK", Codec.stringCodec(100));
        put("ORNI", Codec.stringCodec(30));
        put("SCHU", Codec.stringCodec());
        put("GLNG", Codec.stringCodec());
    }};

    Codec<int[]> DEFAULT_INT_CODEC = new DefaultIntCodec();
    Codec<double[]> DEFAULT_DOUBLE_CODEC = new DefaultDoubleCodec();
    Codec<ByteBuffer[]> DEFAULT_STRING_CODEC = Codec.stringCodec();
}
