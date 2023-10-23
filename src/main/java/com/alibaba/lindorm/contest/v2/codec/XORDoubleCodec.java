package com.alibaba.lindorm.contest.v2.codec;

import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class XORDoubleCodec extends Codec<double[]>{
    @Override
    public void encode(ByteBuffer src, double[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        long pre = Double.doubleToLongBits(data[0]);
        buffer.putLong(pre);
        for (int i = 1; i < data.length; i++) {
            long longValue = Double.doubleToRawLongBits(data[i]);
            long xorValue = longValue ^ pre;
            encodeVarLong(buffer, xorValue);
            pre = longValue;
        }
        buffer.flip();
    }

    @Override
    public double[] decode(ByteBuffer src, int size) {
        double[] data = new double[size];
        BitBuffer buffer = new DirectBitBuffer(src);
        long pre = buffer.getLong();
        data[0] = Double.longBitsToDouble(pre);
        for (int i = 1; i < size; i++) {
            long compressedValue = decodeVarLong(buffer);
            long longValue = compressedValue ^ pre;
            data[i] = Double.longBitsToDouble(longValue);
            pre = longValue;
        }
        return data;
    }

    public static void main(String[] args) {
        XORDoubleCodec varintCodec = new XORDoubleCodec();
//        double[] numbers = {20155.050922867114,20155.050925996657,20155.05093121257,20155.05093851484,20155.050947903474,20155.050959378474,20155.050972939836,20155.05098858756,20155.05100632165,20155.0510261421,20155.051048048914,20155.051072042093,20155.051098121632,20155.051126287533,20155.051156539794};
//        double[] numbers = {-17631.700458325424,-17631.700771279953,-17631.701292870832};
        double[] numbers = {9973.39245805256,9973.397330989092,9973.383986099192,9973.38891858201,9973.371878204334,9973.376779902414,9973.359826356242,9973.357454257546,9973.362384929032,9973.286227238877,9973.291100168366,9973.274285352003,9973.27918715265,9973.26587114058,9973.270772941281};
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
