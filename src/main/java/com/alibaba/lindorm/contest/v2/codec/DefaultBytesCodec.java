package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;

public class DefaultBytesCodec extends Codec<ByteBuffer[]>{
    @Override
    public void encode(ByteBuffer src, ByteBuffer[] data) {
        ByteBuffer encodeBuffer = Context.getCodecEncodeBuffer();
        encodeBuffer.clear();
        for (ByteBuffer value : data) {
            encodeBuffer.put((byte) value.remaining());
            encodeBuffer.put(value);
        }
        encodeBuffer.flip();
        Zstd.compress(src, encodeBuffer);
    }

    @Override
    public ByteBuffer[] decode(ByteBuffer src, int size) {
        ByteBuffer decodeBuffer = Context.getCodecDecodeBuffer();
        decodeBuffer.clear();
        Zstd.decompress(decodeBuffer, src);
        decodeBuffer.flip();

        ByteBuffer[] stringValues = Context.getBlockStringValues();
        for (int i = 0; i < size; i++) {
            ByteBuffer val = ByteBuffer.allocate(decodeBuffer.get());
            decodeBuffer.get(val.array(), 0, val.limit());
            stringValues[i] = val;
        }
        return stringValues;
    }
}
