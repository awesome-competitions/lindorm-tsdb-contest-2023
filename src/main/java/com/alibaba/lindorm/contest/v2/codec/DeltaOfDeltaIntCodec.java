package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaOfDeltaIntCodec extends Codec<int[]>{

    private final int deltaSize;

    private final int deltaSizeBits;

    public DeltaOfDeltaIntCodec(int deltaSize) {
        this.deltaSize = deltaSize;
        this.deltaSizeBits = Util.parseBits(deltaSize, false);
    }

    @Override
    public void encode(ByteBuffer src, int[] data) {
        BitBuffer buffer = new ArrayBitBuffer(data.length * 32L);
        buffer.putInt(data[0]);
        int preDiff = 0;
        for (int i = 1; i < data.length; i++) {
            int diff = data[i] - data[i - 1];
            if (Math.abs(diff - preDiff) > deltaSize ){
                throw new RuntimeException("delta size is too small," + deltaSize + " < " + Math.abs(diff) + " at " + i + "th");
            }
            buffer.putInt(diff - preDiff, deltaSizeBits);
            preDiff = diff;
        }
        buffer.flip();
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = new int[size];
        BitBuffer buffer = new DirectBitBuffer(src);
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
        Codec<int[]> compressor = new DeltaOfDeltaIntCodec(1);
        ByteBuffer src = ByteBuffer.allocate(10);
        compressor.encode(src, new int[]{1,1,2,3,4,6,8,10, 13,16,19});
        src.flip();
        int[] data = compressor.decode(src, 11);
        System.out.println(Arrays.toString(data));
    }
}
