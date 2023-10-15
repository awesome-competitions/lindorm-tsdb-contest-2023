package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaIntCodec extends Codec<Integer>{

    private final int deltaSizeBits;

    public DeltaIntCodec(int deltaSize) {
        this.deltaSizeBits = Util.parseBits(deltaSize, false);
    }

    @Override
    public void encode(ByteBuffer src, Integer[] data) {
        BitBuffer buffer = new ArrayBitBuffer(data.length * 32L);
        buffer.putInt(data[0]);
        for (int i = 1; i < data.length; i++) {
            int diff = data[i] - data[i - 1];
            buffer.put(diff, deltaSizeBits);
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
        for (int i = 1; i < size; i++) {
            int diff = buffer.getInt(deltaSizeBits);
            data[i] = data[i-1] + diff;
        }
        return data;
    }

    public static void main(String[] args) {
        Codec<Integer> compressor = new DeltaIntCodec(1);
        ByteBuffer src = ByteBuffer.allocate(10);
        compressor.encode(src, new Integer[]{-1, -2, -3, -4, -5, -6, -7, -8, -9, -10});
        src.flip();
        Integer[] data = compressor.decode(src, 10);
        System.out.println(Arrays.toString(data));
    }
}
