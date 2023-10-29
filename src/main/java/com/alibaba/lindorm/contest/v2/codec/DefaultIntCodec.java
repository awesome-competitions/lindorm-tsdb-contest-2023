package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;

import java.nio.ByteBuffer;

public class DefaultIntCodec extends Codec<int[]>{
    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        for (int i = 0; i < size; i++) {
            src.putInt(data[i]);
        }
    }

    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        for (int i = 0; i < size; i++) {
            data[i] = src.getInt();
        }
    }
}
