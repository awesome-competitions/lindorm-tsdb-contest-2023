package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;

import java.nio.ByteBuffer;

public class DeltaLongCodec extends Codec<Long>{

    private final int deltaSizeBits;

    public DeltaLongCodec(int deltaSize) {
        this.deltaSizeBits = Util.parseBits(deltaSize, false);
    }

    @Override
    public void encode(ByteBuffer src, Long[] data) {
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
    public Long[] decode(ByteBuffer src, int size) {
        Long[] data = new Long[size];
        BitBuffer buffer = new ArrayBitBuffer(src.array());
        long v = buffer.getLong();
        data[0] = v;
        for (int i = 1; i < size; i++) {
            long diff = buffer.getLong(deltaSizeBits);
            data[i] = data[i-1] + diff;
        }
        return data;
    }
}
