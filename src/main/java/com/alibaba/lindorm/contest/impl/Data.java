package com.alibaba.lindorm.contest.impl;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Data {

    private final FileChannel channel;

    private long readPosition;

    private long writePosition;

    private final ByteBuffer writeBuffer;

    private final ReentrantReadWriteLock lock;

    private final Path path;

    public Data(Path path, OpenOption... options) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, options);
        this.readPosition = channel.size();
        this.writePosition = this.readPosition;
        this.writeBuffer = ByteBuffer.allocateDirect(4 * Const.M);
        this.lock = new ReentrantReadWriteLock();
    }

    public long write(ByteBuffer buffer) throws IOException {
        try{
            this.lock.writeLock().lock();
            long prev = this.readPosition;
            if (this.writeBuffer.remaining() < buffer.remaining()){
                this.writeBuffer.flip();
                this.writePosition += this.writeBuffer.remaining();
                this.channel.write(this.writeBuffer);
                this.writeBuffer.clear();
            }
            this.readPosition += buffer.remaining();
            this.writeBuffer.put(buffer);
            return prev;
        }finally {
            this.lock.writeLock().unlock();
        }
    }

    public void setPosition(long position) throws IOException {
        this.readPosition = position;
        this.writePosition = position;
        this.channel.position(position);
    }

    public int read(ByteBuffer dst, long position, int len) throws IOException {
        try{
            this.lock.readLock().lock();
            dst.limit(len);
            if (this.writePosition > position){
                return this.channel.read(dst, position);
            }else if(this.readPosition > position){
                long srcAddress = Util.getAddress(this.writeBuffer);
                long dstAddress = Util.getAddress(dst);
                Util.copyMemory(srcAddress + position - this.writePosition, dstAddress, dst.limit());
                dst.position(dst.limit());
                return len;
            }
            return 0;
        }finally {
            this.lock.readLock().unlock();
        }
    }

    public void force() throws IOException {
        this.writeBuffer.flip();
        this.writePosition += this.writeBuffer.remaining();
        this.channel.write(this.writeBuffer);
        this.channel.force(false);
        this.writeBuffer.clear();
    }

    public void close() throws IOException {
        this.channel.close();
    }

    public long size() throws IOException {
        return this.channel.size();
    }

    public void delete() throws IOException {
        this.channel.close();
        Files.deleteIfExists(this.path);
    }

    public void move(Path newPath) throws IOException {
        Files.move(path, newPath);
    }

    public interface ThConsumer<A, B, C> {
        void accept(A a, B b, C c) throws IOException;
    }
}