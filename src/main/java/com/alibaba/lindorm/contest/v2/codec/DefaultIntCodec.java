package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;

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
        int[] data = Context.getBlockIntValues();
        for (int i = 0; i < size; i++) {
            data[i] = src.getInt();
        }
        return data;
    }
}
