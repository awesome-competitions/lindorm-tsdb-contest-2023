package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;

public class DeltaLongCodec extends Codec<long[]>{

    private final int deltaSizeBits;

    public DeltaLongCodec(int deltaSize) {
        this.deltaSizeBits = Util.parseBits(deltaSize, false);
    }

    @Override
    public void encode(ByteBuffer src, long[] data) {
        BitBuffer buffer = new ArrayBitBuffer(data.length * 64L);
        buffer.putLong(data[0]);
        for (int i = 1; i < data.length; i++) {
            long diff = data[i] - data[i - 1];
            buffer.put(diff, deltaSizeBits);
        }
        buffer.flip();
        buffer.putToByteBuffer(src);
    }

    @Override
    public long[] decode(ByteBuffer src, int size) {
        long[] data = new long[size];
        BitBuffer buffer = new DirectBitBuffer(src);
        long v = buffer.getLong();
        data[0] = v;
        for (int i = 1; i < size; i++) {
            long diff = buffer.getLong(deltaSizeBits);
            data[i] = data[i-1] + diff;
        }
        return data;
    }
}
