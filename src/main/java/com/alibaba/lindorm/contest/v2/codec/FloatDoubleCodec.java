package com.alibaba.lindorm.contest.v2.codec;

import java.nio.ByteBuffer;

public class FloatDoubleCodec extends Codec<double[]>{
    @Override
    public void encode(ByteBuffer src, double[] data, int size) {
        src.putDouble(data[0]);
        for (int i = 1; i < size; i++) {
            src.putFloat((float)(data[i] - data[0]));
        }
    }

    @Override
    public void decode(ByteBuffer src, double[] data, int size) {
        data[0] = src.getDouble();
        for (int i = 1; i < size; i++) {
            data[i] = data[0] + src.getFloat();
        }
    }
}
