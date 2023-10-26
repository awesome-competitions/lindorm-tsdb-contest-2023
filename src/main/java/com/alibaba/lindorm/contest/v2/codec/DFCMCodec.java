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
            for (int i = 1; i < data.length; i++) {
                long v1 = Double.doubleToRawLongBits(data[i]);
                long v2 = Double.doubleToRawLongBits(data[i - 1]);
                long xorValue = v1 ^ v2;
                int leadingZeros = Long.numberOfLeadingZeros(xorValue);
                int trailingZeros = Long.numberOfTrailingZeros(xorValue);
//                System.out.println(leadingZeros  + ":" + trailingZeros + ":" + xorValue);
                if (xorValue == 0){
                    buffer.putBit(false);
                }else if (preLeadingZeros >= 0 && leadingZeros == preLeadingZeros) {
                    buffer.putBit(true);
                    buffer.putBit(true);
                    buffer.putLong(xorValue, 64 - preLeadingZeros);
                }else {
                    buffer.putBit(true);
                    buffer.putBit(false);
                    buffer.putInt(leadingZeros, 6);
                    buffer.putLong(xorValue, 64 - leadingZeros);
                    preLeadingZeros = leadingZeros;
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
            for (int i = 1; i < size; i++) {
                long xorValue = 0;
                if (buffer.getBoolean()){
                    if (!buffer.getBoolean()) {
                        preLeadingZeros = buffer.getIntUnsigned(6);
                    }
                    xorValue = buffer.getLongUnsigned(64 - preLeadingZeros);
                }
                long v1 = Double.doubleToRawLongBits(data[i-1]);
                data[i] = Double.longBitsToDouble(v1 ^ xorValue);
            }
        }
        return data;
    }

    public static void main(String[] args) {
        DFCMCodec varintCodec = new DFCMCodec();
        double[] numbers = {9973.29309055919, 9973.39245805256,9973.397330989092,9973.383986099192,9973.38891858201,9973.371878204334,9973.376779902414,9973.359826356242,9973.357454257546,9973.362384929032,9973.286227238877,9973.291100168366,9973.274285352003,9973.27918715265,9973.26587114058,9973.270772941281};
//        double[] numbers = {5453148.210216262,5454141.885149957,5454190.614515289,5454057.16561628,5454106.4904444665,5453936.086667698,5453985.103648492,5453815.568186784,5453791.8471998265,5453841.153914697,5453079.577013142,5453128.306308016,5452960.158144395,5453009.176150855,5452876.016030161,5452925.034037175};
        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
        varintCodec.encode(encodedBuffer, numbers);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());

        int size = numbers.length;
        System.out.println(size * 8);
        double[] decodedNumbers = varintCodec.decode(encodedBuffer, size);

        System.out.println(Arrays.toString(decodedNumbers));
    }
}
