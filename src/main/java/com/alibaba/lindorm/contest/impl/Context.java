package com.alibaba.lindorm.contest.impl;

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

    private final BitBuffer writeDataBuffer;

    private final BitBuffer writeTmpBuffer;

    private final BitBuffer readDataBuffer;

    public Context() {
        this.writeDataBuffer = new BitBuffer(Const.MAX_ROW_SIZE);
        this.writeTmpBuffer = new BitBuffer(Const.MAX_ROW_SIZE);
        this.readDataBuffer = new BitBuffer(Const.MAX_ROW_SIZE);
    }

    public BitBuffer getWriteDataBuffer() {
        return writeDataBuffer;
    }

    public BitBuffer getWriteTmpBuffer() {
        return writeTmpBuffer;
    }

    public BitBuffer getReadDataBuffer() {
        return readDataBuffer;
    }
}
