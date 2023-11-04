package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Huffman3Codec extends Codec<int[]>{

    private final int min;

    public Huffman3Codec(int min) {
        this.min = min;
    }

    /**
     * 0	0
     * 1	10
     * 2	11
     */
    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        for (int i = 0; i < size; i++) {
            int v = data[i] - min;
            if (v == 0){
                buffer.putBit(false);
            } else if (v == 1) {
                buffer.putBit(true);
                buffer.putBit(false);
            } else if (v == 2) {
                buffer.putBit(true);
                buffer.putBit(true);
            } else{
                throw new RuntimeException("invalid value " + data[i] + " and min is " + min);
            }
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        for (int i = 0; i < size; i++) {
            if (buffer.getBoolean()) {
                if (buffer.getBoolean()) {
                    data[i] = 2 + min;
                } else {
                    data[i] = 1 + min;
                }
            } else {
                data[i] = min;
            }
        }
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new Huffman3Codec(-1);
        ByteBuffer src = ByteBuffer.allocate(1000);
        compressor.encode(src, new int[]{-1,0,1,1,-1,-1,-1,-1,1,0,-1}, 11);
        src.flip();
        compressor.decode(src, Context.getBlockIntValues(), 11);
        System.out.println(Arrays.toString(Context.getBlockIntValues()));
    }
}
