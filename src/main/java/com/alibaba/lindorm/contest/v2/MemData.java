package com.alibaba.lindorm.contest.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemData extends Data{

    private final ByteBuffer data;

    private final ReentrantReadWriteLock lock;

    public MemData(ByteBuffer data) throws IOException {
        this.data = data;
        this.lock = new ReentrantReadWriteLock();
    }

    public long write(ByteBuffer input) throws IOException {
        throw new UnsupportedOperationException("MemData does not support write operation");
    }

    public int read(ByteBuffer dst, long position, int length) throws IOException {
        dst.limit(length);
        for (int i = 0; i < length; i++) {
            dst.put(data.get((int)position + i));
        }
        return length;
    }

    public void close() throws IOException {
    }

    public long size() throws IOException {
        return data.limit();
    }

    public void delete() throws IOException {

    }

    public void force() throws IOException {
    }
}
