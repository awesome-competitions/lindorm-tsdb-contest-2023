package com.alibaba.lindorm.contest.v2.codec;

import java.nio.ByteBuffer;

public class DefaultIntCodec extends Codec<int[]>{
    @Override
    public void encode(ByteBuffer src, int[] data) {
        for (int value : data) {
            src.putInt(value);
        }
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = src.getInt();
        }
        return data;
    }
}
