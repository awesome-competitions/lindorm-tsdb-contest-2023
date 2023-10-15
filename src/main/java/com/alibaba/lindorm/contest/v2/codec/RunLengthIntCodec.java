package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class RunLengthIntCodec extends Codec<Integer>{

    private final int runLengthMaxSize;

    private final int runLengthSizeBits;

    public RunLengthIntCodec(int runLengthMaxSize) {
        this.runLengthMaxSize = runLengthMaxSize;
        this.runLengthSizeBits = Util.parseBits(runLengthMaxSize, false);
    }

    @Override
    public void encode(ByteBuffer src, Integer[] data) {
        BitBuffer buffer = new ArrayBitBuffer(data.length * 32L);
        int v = data[0];
        int len = 1;
        for (int i = 1; i < data.length; i++) {
           if (data[i] == v && len < runLengthMaxSize){
               len ++;
               continue;
           }
           buffer.putInt(v);
           buffer.putInt(len, runLengthSizeBits);
           v = data[i];
           len = 1;
        }
        buffer.putInt(v);
        buffer.putInt(len, runLengthSizeBits);
        buffer.flip();
        buffer.putToByteBuffer(src);
    }

    @Override
    public Integer[] decode(ByteBuffer src, int size) {
        Integer[] data = new Integer[size];
        BitBuffer buffer = new ArrayBitBuffer(src.array());
        int index = 0;
        while (index < size){
            int v = buffer.getInt();
            int len = buffer.getInt(runLengthSizeBits);
            for (int i = 0; i < len; i++) {
                data[index++] = v;
            }
        }
        return data;
    }

}
