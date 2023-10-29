package com.alibaba.lindorm.contest.v2;

import java.nio.ByteBuffer;

public class Context {

    private static final ThreadLocal<Context> CTX = new ThreadLocal<>();

    /**
     * 获取当前线程上下文
     *
     * @return {@link Context}
     */
    public static Context get(){
        Context ctx = CTX.get();
        if (ctx == null){
            ctx = new Context();
            CTX.set(ctx);
            System.out.println("new thread local context:" + Thread.currentThread().getName());
        }
        return ctx;
    }

    private final ByteBuffer blockWriteBuffer;

    private final ByteBuffer blockReadBuffer;

    private final ByteBuffer codecEncodeBuffer;

    private final ByteBuffer codecDecodeBuffer;

    private final int[] blockIntValues;

    private final double[] blockDoubleValues;

    private final ByteBuffer[] blockStringValues;

    public Context() {
        this.blockWriteBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
        this.blockReadBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
        this.codecEncodeBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
        this.codecDecodeBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
        this.blockIntValues = new int[Const.BLOCK_SIZE];
        this.blockDoubleValues = new double[Const.BLOCK_SIZE];
        this.blockStringValues = new ByteBuffer[Const.BLOCK_SIZE];
    }

    public static ByteBuffer getBlockWriteBuffer() {
        return get().blockWriteBuffer;
    }

    public static ByteBuffer getBlockReadBuffer() {
        return get().blockReadBuffer;
    }

    public static ByteBuffer getCodecEncodeBuffer() {
        return get().codecEncodeBuffer;
    }

    public static ByteBuffer getCodecDecodeBuffer() {
        return get().codecDecodeBuffer;
    }


    public static int[] getBlockIntValues() {
        return get().blockIntValues;
    }

    public static double[] getBlockDoubleValues() {
        return get().blockDoubleValues;
    }

    public static ByteBuffer[] getBlockStringValues() {
        return get().blockStringValues;
    }
}
