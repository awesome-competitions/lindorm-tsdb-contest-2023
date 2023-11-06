package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Data {

    protected Path path;

    public abstract long write(ByteBuffer buffer) throws IOException;

    public abstract int read(ByteBuffer dst, long position, int length) throws IOException;

    public abstract void close() throws IOException;

    public abstract long size() throws IOException;

    public abstract void delete() throws IOException;

    public abstract void force() throws IOException;

    protected Path path() {
        return path;
    }

}