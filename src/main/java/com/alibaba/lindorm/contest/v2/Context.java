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

    private final ByteBuffer dataWriteBuffer;

    private final ByteBuffer dataReadBuffer;

    private final ByteBuffer blockWriteBuffer;

    private final ByteBuffer blockPositionBuffer;

    private final ByteBuffer blockDataBuffer;

    public Context() {
        this.dataWriteBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
        this.dataReadBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
        this.blockWriteBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
        this.blockPositionBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
        this.blockDataBuffer = ByteBuffer.allocateDirect(Const.BYTE_BUFFER_SIZE);
    }

    public static ByteBuffer getDataWriteBuffer() {
        return get().dataWriteBuffer;
    }

    public static ByteBuffer getDataReadBuffer() {
        return get().dataReadBuffer;
    }

    public static ByteBuffer getBlockWriteBuffer() {
        return get().blockWriteBuffer;
    }

    public static ByteBuffer getBlockPositionBuffer() {
        return get().blockPositionBuffer;
    }

    public static ByteBuffer getBlockDataBuffer() {
        return get().blockDataBuffer;
    }
}
