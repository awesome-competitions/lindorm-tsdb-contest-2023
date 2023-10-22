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
            long compressedValue = longValue ^ pre;
            System.out.println(compressedValue);
//            if (compressedValue > Integer.MAX_VALUE){
//                throw new RuntimeException("XOR value is too large, " + compressedValue);
//            }
            encodeVarInt(buffer, (int) compressedValue);
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
            long compressedValue = decodeVarInt(buffer);
            long longValue = compressedValue ^ pre;
            data[i] = Double.longBitsToDouble(longValue);
            pre = longValue;
        }
        return data;
    }

    public static void main(String[] args) {
        XORDoubleCodec varintCodec = new XORDoubleCodec();
//        double[] numbers = {-17631.700458325424,-17631.700771279953,-17631.701292870832,-17631.70202309806,-17631.702961961626,-17631.704109461523,-17631.705465597748,-17631.70703037029,-17631.70880377913,-17631.71078582426,-17631.712976505667,-17631.715375823325,-17631.717983777227,-17631.720800367348,-17631.723825593668};
        double[] numbers = {-17631.700458325424,-17631.700771279953,-17631.701292870832};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
        varintCodec.encode(encodedBuffer, numbers);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());

        int size = numbers.length;
        System.out.println(size);
        System.out.println(15 * 8);
        double[] decodedNumbers = varintCodec.decode(encodedBuffer, size);

        System.out.println(Arrays.toString(decodedNumbers));
    }
}
