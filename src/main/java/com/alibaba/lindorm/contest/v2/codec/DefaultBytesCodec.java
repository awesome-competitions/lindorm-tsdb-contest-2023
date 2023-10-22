package com.alibaba.lindorm.contest.v2.codec;

import java.nio.ByteBuffer;

public class DefaultBytesCodec extends Codec<ByteBuffer>{
    @Override
    public void encode(ByteBuffer src, ByteBuffer data) {
        src.put(data);
    }

    @Override
    public ByteBuffer decode(ByteBuffer src, int size) {
        return src;
    }
}
