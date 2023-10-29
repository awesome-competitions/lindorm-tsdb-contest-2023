package com.alibaba.lindorm.contest.v2.codec;

import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;

public class ZSTDCodec extends Codec<ByteBuffer>{
    @Override
    public void encode(ByteBuffer src, ByteBuffer data, int size) {
        Zstd.compress(src, data);
    }

    @Override
    public void decode(ByteBuffer src, ByteBuffer data, int size) {
        Zstd.decompress(data, src);
    }
}
