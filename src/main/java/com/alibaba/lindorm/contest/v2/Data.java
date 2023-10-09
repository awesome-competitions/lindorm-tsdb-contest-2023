package com.alibaba.lindorm.contest.v2;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public class Data {

    private final FileChannel channel;

    private final Path path;

    private long position;

    public Data(Path path, OpenOption... options) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, options);
        this.position = this.channel.position();
    }

    public synchronized long write(ByteBuffer buffer) throws IOException {
        long pos = this.position;
        this.position += buffer.remaining();
        this.channel.write(buffer);
        return pos;
    }

    public int read(ByteBuffer dst, long position, int size) throws IOException {
        dst.limit(size);
        return this.channel.read(dst, position);
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
        this.channel.force(false);
    }
}