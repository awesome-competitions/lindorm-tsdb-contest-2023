package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;

public class StringRunLengthCodec extends Codec<ByteBuffer[]>{

    private final int runLengthMaxSize;

    private final int runLengthSizeBits;

    public StringRunLengthCodec(int runLengthMaxSize) {
        this.runLengthMaxSize = runLengthMaxSize;
        this.runLengthSizeBits = Util.parseBits(runLengthMaxSize, false);
    }

    @Override
    public void encode(ByteBuffer src, ByteBuffer[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        ByteBuffer v = data[0];
        int count = 1;
        for (int i = 1; i < size; i++) {
            if (data[i].equals(v) && count < runLengthMaxSize){
                count ++;
                continue;
            }
            buffer.putByte((byte) v.remaining());
            buffer.put(v.array(), 0, v.remaining());
            buffer.putInt(count, runLengthSizeBits);
            v = data[i];
            count = 1;
        }
        buffer.putByte((byte) v.remaining());
        buffer.put(v.array(), 0, v.remaining());
        buffer.putInt(count, runLengthSizeBits);
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, ByteBuffer[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        int index = 0;
        while (index < size){
            int len = buffer.getByte();
            byte[] bytes = new byte[len];
            buffer.get(bytes);
            int count = buffer.getInt(runLengthSizeBits);
            for (int i = 0; i < count; i++) {
                data[index++] = ByteBuffer.wrap(bytes);
                if (index == size){
                    break;
                }
            }
        }
    }

    public static void main(String[] args) {
        StringRunLengthCodec bc = new StringRunLengthCodec(600);

        String[] strings = new String[]{"-1", "-1", "-1", "a","a","b","c","c","c"};

        ByteBuffer[] buffers = new ByteBuffer[strings.length];
        for (int i = 0; i < strings.length; i++) {
            buffers[i] = ByteBuffer.wrap(strings[i].getBytes());
        }

        ByteBuffer src = ByteBuffer.allocate(10000);
        bc.encode(src, buffers, buffers.length);
        src.flip();

        System.out.println(src.remaining());

        ByteBuffer[] dst = new ByteBuffer[strings.length];
        bc.decode(src, dst, dst.length);
        for (int i = 0; i < dst.length; i++) {
            System.out.println(new String(dst[i].array()));
        }
    }
}
