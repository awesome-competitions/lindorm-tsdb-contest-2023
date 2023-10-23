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
            System.out.println(xorValue);
            System.out.println(Long.numberOfLeadingZeros(xorValue));
            System.out.println(Long.numberOfTrailingZeros(xorValue));
            System.out.println(xorValue >> Long.numberOfTrailingZeros(xorValue));
            System.out.println("==========================================");
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
        double[] numbers = {20155.050922867114,20155.050925996657,20155.05093121257,20155.05093851484,20155.050947903474,20155.050959378474,20155.050972939836,20155.05098858756,20155.05100632165,20155.0510261421,20155.051048048914,20155.051072042093,20155.051098121632,20155.051126287533,20155.051156539794};
//        double[] numbers = {-17631.700458325424,-17631.700771279953,-17631.701292870832};

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
