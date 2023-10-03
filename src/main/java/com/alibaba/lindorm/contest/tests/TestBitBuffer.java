package com.alibaba.lindorm.contest.tests;


import net.magik6k.bitbuffer.BitBuffer;

import java.nio.ByteBuffer;

public class TestBitBuffer {

    public static void main(String[] args) {
//        long s = System.currentTimeMillis();
//        BitBuffer bf = BitBuffer.allocate(1000);
//        for (long i = 0; i < 1_0000_0000; i ++){
//            bf.putLong(Long.MAX_VALUE, 64);
//            bf.putLong(Integer.MAX_VALUE, 32);
//            bf.clear();
//        }
//        System.out.println(System.currentTimeMillis() - s);


//        long s = System.currentTimeMillis();
//        com.alibaba.lindorm.contest.impl.BitBuffer bf = new com.alibaba.lindorm.contest.impl.BitBuffer(1000);
//        for (long i = 0; i < 1_0000_0000; i ++){
//            bf.putLong(Long.MAX_VALUE, 64);
//            bf.putLong(Integer.MAX_VALUE, 32);
//            bf.clear();
//        }
//        System.out.println(System.currentTimeMillis() - s);

        long s = System.currentTimeMillis();
        com.sun.labs.minion.util.BitBuffer bf = new com.sun.labs.minion.util.BitBuffer(1000);
        for (long i = 0; i < 1_0000_0000; i ++){
            bf.directEncode(Long.MAX_VALUE, 64);
            bf.directEncode(Integer.MAX_VALUE, 32);
            bf.clear();
        }
        System.out.println(System.currentTimeMillis() - s);
//
//
//        long s = System.currentTimeMillis();
//        ByteBuffer bf = ByteBuffer.allocate(100);
//        for (long i = 0; i < 1000_0000 * 60L; i ++){
//            bf.putLong(Long.MAX_VALUE);
//            bf.putInt(Integer.MAX_VALUE);
//            bf.clear();
//        }
//        System.out.println(System.currentTimeMillis() - s);
    }
}
