package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DFCMIntCodec extends Codec<int[]>{
    @Override
    public void encode(ByteBuffer src, int[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        int preLeadingZeros = -1;
        for (int i = 0; i < data.length; i++) {
            int leadingZeros = Integer.numberOfLeadingZeros(data[i]);
            if (preLeadingZeros >= 0 && leadingZeros == preLeadingZeros) {
                buffer.putBit(true);
                buffer.putInt(data[i], 32 - preLeadingZeros);
            }else {
                buffer.putBit(false);
                buffer.putInt(leadingZeros, 5);
                buffer.putInt(data[i], 32 - leadingZeros);
                preLeadingZeros = leadingZeros;
            }
        }
        buffer.flip();
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = Context.getBlockIntValues();
        BitBuffer buffer = new DirectBitBuffer(src);
        int preLeadingZeros = -1;
        for (int i = 0; i < size; i++) {
            if (!buffer.getBoolean()) {
                preLeadingZeros = buffer.getIntUnsigned(5);
            }
            int v = buffer.getIntUnsigned(32 - preLeadingZeros);
            data[i] = v;
        }
        return data;
    }

    public static void main(String[] args) {
        DFCMIntCodec varintCodec = new DFCMIntCodec();
//        int[] numbers = {47508323,38447491,9735651,48158467,31176995,21444931,30686115,41353795,3464675,12283267,16355299,22657539,37193507,44959939,46392611,4670915};
//        int[] numbers = {-11484,-8668,-10508,-17692,-9164,-11228,-13692,-12364,-15132,-12892,-10860,-8620,-12652,-16220,-13548,-15244};
        int[] numbers = {1364896,287712,1075872,1498688,1017216,285152,1042688,194016,821248,1123488,695520,997760,33728,300160,732832,1511136};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
        varintCodec.encode(encodedBuffer, numbers);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());
        System.out.println(numbers.length * 4);

        int size = numbers.length;
        int[] decodedNumbers = varintCodec.decode(encodedBuffer, size);
        System.out.println(Arrays.toString(decodedNumbers));
    }
}
