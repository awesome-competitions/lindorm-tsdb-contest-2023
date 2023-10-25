package com.alibaba.lindorm.contest.v2.codec;

import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DFCMCodec extends Codec<double[]>{
    @Override
    public void encode(ByteBuffer src, double[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putDouble(data[0]);
        if (data.length > 1){
            int preLeadingZeros = -1;
            int preTrailingZeros = -1;
            for (int i = 1; i < data.length; i++) {
                long v1 = Double.doubleToRawLongBits(data[i]);
                long v2 = Double.doubleToRawLongBits(data[i - 1]);
                long xorValue = v1 ^ v2;
                int leadingZeros = Long.numberOfLeadingZeros(xorValue);
                int trailingZeros = Long.numberOfTrailingZeros(xorValue);
                if (xorValue == 0){
                    buffer.putBit(false);
                }else if (preTrailingZeros >= 0 && leadingZeros >= preLeadingZeros && trailingZeros >= preTrailingZeros) {
                    buffer.putBit(true);
                    buffer.putBit(true);
                    long v = xorValue >>> preTrailingZeros;
                    buffer.putLong(v, 64 - preLeadingZeros - preTrailingZeros);
                }else {
                    buffer.putBit(true);
                    buffer.putBit(false);
                    buffer.putInt(leadingZeros, 6);
                    buffer.putInt(trailingZeros, 6);
                    long v = xorValue >>> trailingZeros;
                    buffer.putLong(v, 64 - leadingZeros - trailingZeros);
                    preLeadingZeros = leadingZeros;
                    preTrailingZeros = trailingZeros;
                }
            }
        }
        buffer.flip();
    }

    @Override
    public double[] decode(ByteBuffer src, int size) {
        double[] data = new double[size];
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = buffer.getDouble();
        if (size > 1){
            int preLeadingZeros = -1;
            int preTrailingZeros = -1;
            for (int i = 1; i < size; i++) {
                long xorValue = 0;
                if (buffer.getBoolean()){
                    if (!buffer.getBoolean()) {
                        preLeadingZeros = buffer.getIntUnsigned(6);
                        preTrailingZeros = buffer.getIntUnsigned(6);
                    }
                    long v = buffer.getLongUnsigned(64 - preLeadingZeros - preTrailingZeros);
                    xorValue = v << preTrailingZeros;
                }
                long v1 = Double.doubleToRawLongBits(data[i-1]);
                data[i] = Double.longBitsToDouble(v1 ^ xorValue);
            }
        }
        return data;
    }
}
