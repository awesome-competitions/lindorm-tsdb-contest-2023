package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class FixedIntCodec extends Codec<int[]>{

    private final int min;

    private final int max;

    private final int bits;

    public FixedIntCodec(int min, int max) {
        this.min = min;
        this.max = max;
        this.bits = Util.parseBits(max - min, true);
    }

    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        for (int i = 0; i < size; i++) {
            buffer.putInt(data[i] - min, bits);
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        for (int i = 0; i < size; i++) {
            data[i] = buffer.getIntUnsigned(bits) + min;
        }
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new FixedIntCodec(1,9);
        ByteBuffer src = ByteBuffer.allocate(9);
        compressor.encode(src, new int[]{1,2,3,4,5,6,7,8,9}, 9);
        src.flip();
        compressor.decode(src, Context.getBlockIntValues(), 9);
        System.out.println(Arrays.toString(Context.getBlockIntValues()));
    }
}
