package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaIntCodec extends Codec<int[]>{

    private final int deltaSize;

    private final int deltaSizeBits;

    public DeltaIntCodec(int deltaSize) {
        this.deltaSize = deltaSize;
        this.deltaSizeBits = Util.parseBits(deltaSize, false);
    }

    @Override
    public void encode(ByteBuffer src, int[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        encodeVarInt(buffer, data[0]);
        for (int i = 1; i < data.length; i++) {
            int diff = data[i] - data[i - 1];
            if (Math.abs(diff) > deltaSize ){
                throw new RuntimeException("delta size is too small," + deltaSize + " < " + Math.abs(diff) + " at " + i + "th");
            }
            buffer.put(diff, deltaSizeBits);
        }
        buffer.flip();
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = new int[size];
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = decodeVarInt(buffer);
        for (int i = 1; i < size; i++) {
            int diff = buffer.getInt(deltaSizeBits);
            data[i] = data[i-1] + diff;
        }
        return data;
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new DeltaIntCodec(15);
        ByteBuffer src = ByteBuffer.allocate(10);
        compressor.encode(src, new int[]{-1, -2, -3, -4, -5, -6, -7, -8, -9, -10});
        src.flip();
        int[] data = compressor.decode(src, 10);
        System.out.println(Arrays.toString(data));
    }
}
