package com.alibaba.lindorm.contest.v2.tests;

import com.alibaba.lindorm.contest.v2.Context;
import com.alibaba.lindorm.contest.v2.codec.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestDFCMInt {

    public static void main(String[] args) {

        List<Codec<int[]>> codecs = new ArrayList<>();
        codecs.add(new VarIntCodec());
        codecs.add(new DeltaVarIntCodec());
        codecs.add(new DFCMIntCodec());
        codecs.add(new BDFCMIntCodec());
        codecs.add(new SimpleNBCodec());

//        int[] numbers = {1364896,287712,1075872,1498688,1017216,285152,1042688,194016,821248,1123488,695520,997760,33728,300160,732832,1511136};
//        int[] numbers = {5122526,3545342,4833502,3256318,6274846,6542782,8300318,6451646,1078878,7381118,1453150,7755390,2291358,57790,1490462,9768766};
//        int[] numbers = {411419,834235,122395,1061563,1563739,4348027,1589211,3740539,4367771,4670011,2742043,3560635,3580251,5346683,3295707,5574011};
        int[] numbers = {47508323,38447491,9735651,48158467,31176995,21444931,30686115,41353795,3464675,12283267,16355299,22657539,37193507,44959939,46392611,4670915};

        for (Codec<int[]> codec : codecs) {
            System.out.println(codec.getClass().getSimpleName());
            ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
            codec.encode(encodedBuffer, numbers, numbers.length);

            encodedBuffer.flip();
            System.out.println(encodedBuffer.remaining());

            int size = numbers.length;
            System.out.println(size * 4);
            codec.decode(encodedBuffer, Context.getBlockIntValues(), size);
            System.out.println(Arrays.toString(Context.getBlockIntValues()));
        }
    }
}
