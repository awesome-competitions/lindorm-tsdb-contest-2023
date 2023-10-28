package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;

import java.nio.ByteBuffer;

public class DefaultDoubleCodec extends Codec<double[]>{
    @Override
    public void encode(ByteBuffer src, double[] data) {
        for (double value : data) {
            src.putDouble(value);
        }
    }

    @Override
    public double[] decode(ByteBuffer src, int size) {
        double[] data = Context.getBlockDoubleValues();
        for (int i = 0; i < size; i++) {
            data[i] = src.getDouble();
        }
        return data;
    }
}
