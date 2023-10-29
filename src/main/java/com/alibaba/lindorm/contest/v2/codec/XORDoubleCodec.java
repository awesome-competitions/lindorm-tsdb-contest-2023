package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class XORDoubleCodec extends Codec<double[]>{
    @Override
    public void encode(ByteBuffer src, double[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        long pre = Double.doubleToLongBits(data[0]);
        buffer.putLong(pre);
        for (int i = 1; i < size; i++) {
            long longValue = Double.doubleToRawLongBits(data[i]);
            long xorValue = longValue ^ pre;
            encodeVarLong(buffer, xorValue);
            pre = longValue;
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, double[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        long pre = buffer.getLong();
        data[0] = Double.longBitsToDouble(pre);
        for (int i = 1; i < size; i++) {
            long compressedValue = decodeVarLong(buffer);
            long longValue = compressedValue ^ pre;
            data[i] = Double.longBitsToDouble(longValue);
            pre = longValue;
        }
    }

    public static void main(String[] args) {
        XORDoubleCodec varintCodec = new XORDoubleCodec();
        double[][] numbersList = {
                {9973.29309055919, 9973.39245805256,9973.397330989092,9973.383986099192,9973.38891858201,9973.371878204334,9973.376779902414,9973.359826356242,9973.357454257546,9973.362384929032,9973.286227238877,9973.291100168366,9973.274285352003,9973.27918715265,9973.26587114058,9973.270772941281},
                {5453148.210216262,5454141.885149957,5454190.614515289,5454057.16561628,5454106.4904444665,5453936.086667698,5453985.103648492,5453815.568186784,5453791.8471998265,5453841.153914697,5453079.577013142,5453128.306308016,5452960.158144395,5453009.176150855,5452876.016030161,5452925.034037175},
                {50633949.88139567,50624013.13205872,50623525.8384054,50624860.32739549,50624367.079113625,50626071.11688131,50625580.94707336,50627276.301690444,50627513.511560015,50627020.44441133,50634636.213426866,50634148.92047813,50635830.40211433,50635340.222049735,50636671.82325668,50636181.64318655},
                {9493.598575277196,9490.116340839557,9489.94557365169,9490.413231973373,9490.240378045479,9490.837541040784,9490.665765933289,9491.259886005302,9491.343013831993,9491.170223380403,9493.83909347421,9493.668326533254,9494.257584957846,9494.085806256011,9494.552452582973,9494.380673879194},
                {154476.05679147458,154491.5267799328,154492.2854211078,154490.20782726153,154490.97573887615,154488.3228144937,154489.0859333985,154486.44652734432,154486.07722810772,154486.8448577259,154474.98827819715,154475.74691827522,154473.12911031581,154473.89224518865,154471.81914717602,154472.5822820575}
        };

        for (double[] numbers: numbersList){
            ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
            varintCodec.encode(encodedBuffer, numbers, numbers.length);

            encodedBuffer.flip();
            System.out.println(encodedBuffer.remaining());

            int size = numbers.length;
            System.out.println(size * 8);
            varintCodec.decode(encodedBuffer, Context.getBlockDoubleValues(), size);

            System.out.println(Arrays.toString(Context.getBlockDoubleValues()));
        }
    }
}
