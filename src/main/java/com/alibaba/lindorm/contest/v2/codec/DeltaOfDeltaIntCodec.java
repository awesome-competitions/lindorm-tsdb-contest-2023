package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaOfDeltaIntCodec extends Codec<Integer>{

    private final int deltaSizeBits;

    public DeltaOfDeltaIntCodec(int deltaSize) {
        this.deltaSizeBits = Util.parseBits(deltaSize, false);
    }

    @Override
    public void encode(ByteBuffer src, Integer[] data) {
        BitBuffer buffer = new ArrayBitBuffer(data.length * 32L);
        buffer.putInt(data[0]);
        int preDiff = 0;
        for (int i = 1; i < data.length; i++) {
            int diff = data[i] - data[i - 1];
            buffer.putInt(diff - preDiff, deltaSizeBits);
            preDiff = diff;
        }
        buffer.flip();
        buffer.putToByteBuffer(src);
    }

    @Override
    public Integer[] decode(ByteBuffer src, int size) {
        Integer[] data = new Integer[size];
        BitBuffer buffer = new ArrayBitBuffer(src.array());
        int v = buffer.getInt();
        data[0] = v;
        int preDiff = 0;
        for (int i = 1; i < size; i++) {
            int diff = buffer.getInt(deltaSizeBits) + preDiff;
            data[i] = data[i-1] + diff;
            preDiff = diff;
        }
        return data;
    }

    public static void main(String[] args) {
        Codec<Integer> compressor = new DeltaOfDeltaIntCodec(1);
        ByteBuffer src = ByteBuffer.allocate(10);
        compressor.encode(src, new Integer[]{1,1,2,3,4,6,8,10, 13,16,19});
        src.flip();
        Integer[] data = compressor.decode(src, 11);
        System.out.println(Arrays.toString(data));
    }
}
