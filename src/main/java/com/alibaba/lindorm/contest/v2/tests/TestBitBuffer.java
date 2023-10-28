package com.alibaba.lindorm.contest.v2.tests;

import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;

public class TestBitBuffer {

    public static void main(String[] args) {
        BitBuffer buffer = new ArrayBitBuffer(1132);
        buffer.putInt(Integer.MAX_VALUE, 31);
        buffer.flip();
        System.out.println(buffer.getIntUnsigned(31));
    }
}
