package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaVarIntCodec extends Codec<int[]>{
    public DeltaVarIntCodec() {}

    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putInt(data[0]);
        for (int i = 1; i < size; i++) {
            int diff = data[i] - data[i - 1];
            encodeVarInt(buffer, diff);
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        int v = buffer.getInt();
        data[0] = v;
        for (int i = 1; i < size; i++) {
            int diff = decodeVarInt(buffer);
            data[i] = data[i-1] + diff;
        }
    }

    public static void main(String[] args) {
        DeltaVarIntCodec varintCodec = new DeltaVarIntCodec();
//        int[] numbers = {-13061,-14901,-22085,-13557,-15621,-18085,-16757,-19525,-17285,-15253,-13013,-17045,-20613,-17941,-13285,-19381};
//        int[] numbers = {47508323,38447491,9735651,48158467,31176995,21444931,30686115,41353795,3464675,12283267,16355299,22657539,37193507,44959939,46392611,4670915};
//        int[] numbers = {-11484,-8668,-10508,-17692,-9164,-11228,-13692,-12364,-15132,-12892,-10860,-8620,-12652,-16220,-13548,-15244};
        int[] numbers = {1364896,287712,1075872,1498688,1017216,285152,1042688,194016,821248,1123488,695520,997760,33728,300160,732832,1511136};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
        varintCodec.encode(encodedBuffer, numbers, numbers.length);

        encodedBuffer.flip();

        System.out.println(encodedBuffer.remaining());
        System.out.println(numbers.length * 4);

        int size = numbers.length;
       varintCodec.decode(encodedBuffer, Context.getBlockIntValues(), size);
        System.out.println(Arrays.toString(Context.getBlockIntValues()));
    }
}
