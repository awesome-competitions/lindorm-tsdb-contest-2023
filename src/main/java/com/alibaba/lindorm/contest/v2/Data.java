package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Data {

    private final FileChannel channel;

    private final Path path;

    private long readPosition;

    private long writePosition;

    private final ByteBuffer writeBuffer;

    private final ReentrantReadWriteLock lock;

    public Data(Path path, OpenOption... options) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, options);
        this.readPosition = this.channel.size();
        this.writePosition = this.readPosition;
        this.writeBuffer = ByteBuffer.allocateDirect(Const.DATA_BUFFER_SIZE);
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

    public int read(ByteBuffer dst, long position, int length) throws IOException {
        try{
            this.lock.readLock().lock();
            dst.limit(length);
            if (this.writePosition > position){
                return this.channel.read(dst, position);
            }else if(this.readPosition > position){
                long srcAddress = Util.getAddress(this.writeBuffer);
                long dstAddress = Util.getAddress(dst);
                Util.copyMemory(srcAddress + position - this.writePosition, dstAddress, dst.limit());
                dst.position(dst.limit());
                return length;
            }
            return 0;
        }finally {
            this.lock.readLock().unlock();
        }
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

    public void force() throws IOException {
        this.writeBuffer.flip();
        this.writePosition += this.writeBuffer.remaining();
        this.channel.write(this.writeBuffer);
        this.channel.force(false);
        this.writeBuffer.clear();
    }
}