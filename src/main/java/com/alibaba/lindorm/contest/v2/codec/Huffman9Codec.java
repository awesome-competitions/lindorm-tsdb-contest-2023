package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Huffman9Codec extends Codec<int[]>{

    private final int min;

    public Huffman9Codec(int min) {
        this.min = min;
    }

    /**
     * 0	110
     * 1	1111
     * 2	1110
     * 3	001
     * 4	000
     * 5	011
     * 6	010
     * 7	101
     * 8	100
     */
    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        for (int i = 0; i < size; i++) {
            int v = data[i] - min;
            if(v == 0){
                buffer.putBit(true);
                buffer.putBit(true);
                buffer.putBit(false);
            } else if (v == 1) {
                buffer.putBit(true);
                buffer.putBit(true);
                buffer.putBit(true);
                buffer.putBit(true);
            } else if (v == 2) {
                buffer.putBit(true);
                buffer.putBit(true);
                buffer.putBit(true);
                buffer.putBit(false);
            } else if (v == 3) {
                buffer.putBit(false);
                buffer.putBit(false);
                buffer.putBit(true);
            } else if (v == 4) {
                buffer.putBit(false);
                buffer.putBit(false);
                buffer.putBit(false);
            } else if (v == 5) {
                buffer.putBit(false);
                buffer.putBit(true);
                buffer.putBit(true);
            } else if (v == 6) {
                buffer.putBit(false);
                buffer.putBit(true);
                buffer.putBit(false);
            } else if (v == 7) {
                buffer.putBit(true);
                buffer.putBit(false);
                buffer.putBit(true);
            } else if (v == 8) {
                buffer.putBit(true);
                buffer.putBit(false);
                buffer.putBit(false);
            } else{
                throw new RuntimeException("invalid value " + data[i] + " and min is " + min);
            }
        }
        buffer.flip();
    }

    /**
     * 0	110
     * 1	1111
     * 2	1110
     * 3	001
     * 4	000
     * 5	011
     * 6	010
     * 7	101
     * 8	100
     */
    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        for (int i = 0; i < size; i++) {
            if (buffer.getBoolean()) {
                if (buffer.getBoolean()) {
                    if (buffer.getBoolean()) {
                        if (buffer.getBoolean()) {
                            data[i] = 1 + min;
                        } else {
                            data[i] = 2 + min;
                        }
                    } else {
                        data[i] = min;
                    }
                } else {
                    if (buffer.getBoolean()) {
                        data[i] = 7 + min;
                    } else {
                        data[i] = 8 + min;
                    }
                }
            } else {
                if (buffer.getBoolean()) {
                    if (buffer.getBoolean()) {
                        data[i] = 5 + min;
                    } else {
                        data[i] = 6 + min;
                    }
                } else {
                    if (buffer.getBoolean()) {
                        data[i] = 3 + min;
                    } else {
                        data[i] = 4 + min;
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new Huffman9Codec(1);
        ByteBuffer src = ByteBuffer.allocate(1000);
        compressor.encode(src, new int[]{1,2,3,5,5,6,7,8,9,4,3}, 11);
        src.flip();
        compressor.decode(src, Context.getBlockIntValues(), 11);
        System.out.println(Arrays.toString(Context.getBlockIntValues()));
    }
}
