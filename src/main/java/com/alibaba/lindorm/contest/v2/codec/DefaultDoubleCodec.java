package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;

import java.nio.ByteBuffer;

public class DefaultDoubleCodec extends Codec<double[]>{
    @Override
    public void encode(ByteBuffer src, double[] data, int size) {
        for (int i = 0; i < size; i++) {
            src.putDouble(data[i]);
        }
    }

    @Override
    public void decode(ByteBuffer src, double[] data, int size) {
        for (int i = 0; i < size; i++) {
            data[i] = src.getDouble();
        }
    }
}
