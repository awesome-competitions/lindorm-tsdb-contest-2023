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

    int K = 1024;
    int M = 1024 * K;
    int G = 1024 * M;
    int BYTE_BUFFER_SIZE = 128 * K;
    int DATA_BUFFER_SIZE = 512 * K;

    // vin
    int VIN_COUNT = 5000;
    int VIN_LENGTH = 17;

    // block size
    int BLOCK_SIZE = 400;
    // the time span of all the data
    int TIME_SPAN = 60 * 60 * 10;

    int TIMESTAMP_INTERVAL = 1000;

    // column
    int COLUMN_COUNT = 60;
    int INT_COLUMN_COUNT = 40;
    int DOUBLE_COLUMN_COUNT = 10;
    int STRING_COLUMN_COUNT = 10;
    List<String> ALL_COLUMNS = new ArrayList<>(COLUMN_COUNT);
    List<String> INT_COLUMNS = new ArrayList<>(INT_COLUMN_COUNT);
    List<String> DOUBLE_COLUMNS = new ArrayList<>(DOUBLE_COLUMN_COUNT);
    List<String> STRING_COLUMNS = new ArrayList<>(STRING_COLUMN_COUNT);

    Map<String, Column> COLUMNS_INDEX = new HashMap<>(COLUMN_COUNT);

    // compress columns
    Set<String> COMPRESS_COLUMNS = new HashSet<>(){{
        add("GMVS");
        add("LIOS");
        add("LFZG");
        add("WLYQ");
        add("ZTGP");
        add("DSKQ");
        add("GMNR");
        add("BTJH");
        add("XCDQ");
        add("JKZL");
        add("MLBY");
        add("EQLU");
        add("KACE");
        add("YDXB");
        add("NXCR");
        add("OZSM");
        add("SYMN");
        add("UAPJ");
        add("PQRQ");
        add("UICP");
        add("WKUZ");
//        add("AOAO");
//        add("EXGV");
//        add("JCGU");
//        add("TFGW");
//        add("MUBC");
//        add("JHET");
//        add("YCTI");
//        add("BBPX");
//        add("HRXI");

        add("FWKW");
//        add("BDPO");
//        add("FQTH");
//        add("SBXA");
//        add("XRTP");

//        add("FVXS");
//        add("LIYD");
//        add("ZEBY");
//        add("UFPI");
    }};

    // coding
    Map<String, Codec<int[]>> COLUMNS_INTEGER_CODEC = new HashMap<>(COLUMN_COUNT){{
        // Integer
        put("GMVS", Codec.runLengthIntCodec(BLOCK_SIZE));
        put("LIOS", Codec.huffmanDeltaOfDeltaIntCodec());
        put("LFZG", Codec.huffmanDeltaOfDeltaIntCodec());
        put("WLYQ", Codec.huffmanDeltaOfDeltaIntCodec());
        put("ZTGP", Codec.huffmanDeltaOfDeltaIntCodec());
        put("DSKQ", Codec.huffmanDeltaOfDeltaIntCodec());
        put("GMNR", Codec.huffmanDeltaOfDeltaIntCodec());
        put("BTJH", Codec.huffmanDeltaOfDeltaIntCodec());
        put("XCDQ", Codec.huffmanDeltaOfDeltaIntCodec());
        put("JKZL", Codec.huffmanDeltaOfDeltaIntCodec());
        put("MLBY", Codec.huffmanDeltaOfDeltaIntCodec());
        put("EQLU", Codec.huffmanDeltaOfDeltaIntCodec());
        put("KACE", Codec.huffmanDeltaOfDeltaIntCodec());
        put("YDXB", Codec.huffmanDeltaOfDeltaIntCodec());
        put("NXCR", Codec.huffmanDeltaOfDeltaIntCodec());
        put("OZSM", Codec.huffmanDeltaOfDeltaIntCodec());
        put("SYMN", Codec.huffmanDeltaOfDeltaIntCodec());
        put("UAPJ", Codec.huffmanDeltaOfDeltaIntCodec());
        put("PQRQ", Codec.huffmanDeltaOfDeltaIntCodec());
        put("UICP", Codec.huffmanDeltaOfDeltaIntCodec());
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
        put("AOAO", Codec.huffmanCodec(-2147483647, -2147483645));
        put("EXGV", Codec.huffmanCodec(-2147483647, -2147483645));
        put("JCGU", Codec.huffmanCodec(-2147483647, -2147483645));
        put("TFGW", Codec.huffmanCodec(-2147483647, -2147483645));
        put("MUBC", Codec.huffmanCodec(-2147483647, -2147483645));
        put("JHET", Codec.huffmanCodec(1, 3));
        put("WKUZ", Codec.runLengthIntCodec(BLOCK_SIZE));
        put("YCTI", Codec.huffmanCodec(1, 5));
        put("BBPX", Codec.huffmanCodec(1, 9));
        put("HRXI", Codec.huffmanCodec(1, 9));
    }};

    Map<String, Codec<double[]>> COLUMNS_DOUBLE_CODEC = new HashMap<>(COLUMN_COUNT){{
        put("FWKW", Codec.deltaOfDeltaBDFCMPlusCodec());
        put("BDPO", Codec.deltaOfDeltaBDFCMPlusCodec());
        put("FQTH", Codec.deltaOfDeltaBDFCMPlusCodec());
        put("SBXA", Codec.deltaOfDeltaBDFCMPlusCodec());
        put("XRTP", Codec.deltaOfDeltaBDFCMPlusCodec());
        put("ZIKG", Codec.simpleBDFCMCodec());
        put("LMLK", Codec.simpleBDFCMCodec());
        put("TEDW", Codec.simpleBDFCMCodec());
        put("UVGJ", Codec.simpleBDFCMCodec());
        put("LYLI", Codec.simpleBDFCMCodec());
    }};

    Map<String, Codec<ByteBuffer[]>> COLUMNS_STRING_CODEC = new HashMap<>(COLUMN_COUNT){{
        put("FVXS", Codec.stringHuffman3Codec(-1));
        put("LIYD", Codec.stringRunLengthCodec(BLOCK_SIZE));
        put("ZEBY", Codec.stringRunLengthCodec(BLOCK_SIZE));
        put("UFPI", Codec.stringRunLengthCodec(BLOCK_SIZE));
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
