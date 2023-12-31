package com.alibaba.lindorm.contest.v1;

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

    private final ByteBuffer writeDataBuffer;

    private final ByteBuffer readDataBuffer;

    public Context() {
        this.writeDataBuffer = ByteBuffer.allocateDirect(Const.MAX_ROW_SIZE);
        this.readDataBuffer = ByteBuffer.allocateDirect(Const.MAX_ROW_SIZE);
    }

    public ByteBuffer getWriteDataBuffer() {
        return writeDataBuffer;
    }

    public ByteBuffer getReadDataBuffer() {
        return readDataBuffer;
    }
}
