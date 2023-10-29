package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;

public class DefaultBytesCodec extends Codec<ByteBuffer[]>{

    @Override
    public void encode(ByteBuffer src, ByteBuffer[] data, int size) {
        ByteBuffer encodeBuffer = Context.getCodecEncodeBuffer();
        encodeBuffer.clear();
        for (int i = 0; i < size; i++) {
            ByteBuffer value = data[i];
            encodeBuffer.put((byte) value.remaining());
            encodeBuffer.put(value);
        }
        encodeBuffer.flip();
        Zstd.compress(src, encodeBuffer);
    }

    @Override
    public void decode(ByteBuffer src, ByteBuffer[] data, int size) {
        ByteBuffer decodeBuffer = Context.getCodecDecodeBuffer();
        decodeBuffer.clear();
        Zstd.decompress(decodeBuffer, src);
        decodeBuffer.flip();

        for (int i = 0; i < size; i++) {
            ByteBuffer val = ByteBuffer.allocate(decodeBuffer.get());
            decodeBuffer.get(val.array(), 0, val.limit());
            data[i] = val;
        }
    }
}
