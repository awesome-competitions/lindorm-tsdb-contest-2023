package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SimpleNBLongCodec extends Codec<long[]>{
    @Override
    public void encode(ByteBuffer src, long[] data, int size) {
        long minValue = Integer.MAX_VALUE;
        long maxValue = Integer.MIN_VALUE;
        for (int i = 0; i < size; i++) {
            if (data[i] < minValue){
                minValue = data[i];
            }
            if (data[i] > maxValue){
                maxValue = data[i];
            }
        }
        int maxBits = Util.parseBits(maxValue - minValue, true);
        BitBuffer buffer = new DirectBitBuffer(src);
        encodeVarLong(buffer, minValue);
        buffer.putInt(maxBits, 6);
        for (long value : data) {
            buffer.putLong(value - minValue, maxBits);
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, long[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        long min = decodeVarLong(buffer);
        int bits = buffer.getIntUnsigned(6);
        for (int i = 0; i < size; i++) {
            data[i] = buffer.getLongUnsigned(bits) + min;
        }
    }

    public static void main(String[] args) {
        SimpleNBLongCodec varintCodec = new SimpleNBLongCodec();
//        int[] numbers = {47508323,38447491,9735651,48158467,31176995,21444931,30686115,41353795,3464675,12283267,16355299,22657539,37193507,44959939,46392611,4670915};
        long[] numbers = {1364896,287712,1075872,1498688,1017216,285152,1042688,194016,821248,1123488,695520,997760,33728,300160,732832,1511136};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
        varintCodec.encode(encodedBuffer, numbers, numbers.length);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());
        System.out.println(numbers.length * 4);

        int size = numbers.length;

        long[] longs = new long[1000];
        varintCodec.decode(encodedBuffer,longs, size);
        System.out.println(Arrays.toString(longs));
    }
}
