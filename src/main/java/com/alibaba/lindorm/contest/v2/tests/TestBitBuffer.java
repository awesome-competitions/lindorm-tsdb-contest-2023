package com.alibaba.lindorm.contest.v2.tests;

import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;

public class TestBitBuffer {

    public static void main(String[] args) {
        ByteBuffer src = ByteBuffer.allocateDirect(2);
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putInt(1, 7);
        buffer.flip();

        System.out.println(src.position());

        BitBuffer buffer2 = new DirectBitBuffer(src);
        buffer2.putInt(2, 7);
        buffer2.flip();

        System.out.println(src.position());

        src.flip();
        BitBuffer buffer3 = new DirectBitBuffer(src);
        System.out.println(buffer3.getInt(7));
        System.out.println(buffer3.getInt(1));
        System.out.println(buffer3.getInt(7));
    }
}
