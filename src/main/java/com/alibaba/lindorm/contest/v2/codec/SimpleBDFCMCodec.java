package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

public class SimpleBDFCMCodec extends Codec<double[]>{

    @Override
    public void encode(ByteBuffer src, double[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putDouble(data[0]);
        if (data.length > 1){
            int minLeadingZeros = 32;
            int maxLeadingZeros = 0;
            for (int i = 1; i < size; i++) {
                long v1 = Double.doubleToRawLongBits(data[i]);
                long v2 = Double.doubleToRawLongBits(data[i - 1]);
                long xorValue = v1 ^ v2;
                int leadingZeros = Long.numberOfLeadingZeros(xorValue) / 3;
                if (leadingZeros < minLeadingZeros){
                    minLeadingZeros = leadingZeros;
                }
                if (leadingZeros > maxLeadingZeros){
                    maxLeadingZeros = leadingZeros;
                }
            }
            int leadingZerosDeltaBits = Util.parseBits(maxLeadingZeros - minLeadingZeros, true);
            buffer.putInt(minLeadingZeros, 4);
            buffer.putInt(leadingZerosDeltaBits, 4);

            for (int i = 1; i < size; i++) {
                long v1 = Double.doubleToRawLongBits(data[i]);
                long v2 = Double.doubleToRawLongBits(data[i - 1]);
                long xorValue = v1 ^ v2;
                int leadingZeros = Long.numberOfLeadingZeros(xorValue) / 3;
                buffer.putInt(leadingZeros - minLeadingZeros, leadingZerosDeltaBits);
                buffer.putLong(xorValue, 64 - leadingZeros * 3);
            }
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, double[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = buffer.getDouble();
        if (size > 1){
            int minLeadingZeros = buffer.getIntUnsigned(4);
            int leadingZerosDeltaBits = buffer.getIntUnsigned(4);
            for (int i = 1; i < size; i++) {
                int leadingZeros = buffer.getIntUnsigned(leadingZerosDeltaBits) + minLeadingZeros;
                long xorValue = buffer.getLongUnsigned(64 - leadingZeros * 3);
                long v1 = Double.doubleToRawLongBits(data[i-1]);
                data[i] = Double.longBitsToDouble(v1 ^ xorValue);
            }
        }
    }

    public static void main(String[] args) {
        SimpleBDFCMCodec varintCodec = new SimpleBDFCMCodec();
        double[][] numbersList = {
                {5453214.744962816,5453044.359296671,5453057.866008186,5452959.062308822,5452974.301314467,5453466.793735752,5453444.211729925,5453345.697835388,5453323.148604929,5453224.076427234,5453200.337395937,5453178.044031174,5453079.240402496,5453631.714510954,5452942.498935414,5452956.871740541,5452858.3568196  ,5452835.792791409,5452700.901503362,5452749.91732521 ,5452579.515799924,5452591.883494316,5452569.301490623,5453062.641997589,5453040.059993247,5452941.274338493,5452991.735961638,5453394.132821335,5453442.862184492,5453274.715047356,5453324.019645315,5453264.482690508,5452574.400985392,5452552.703085199,5452453.899456385,5452467.406131527,5452297.002421041,5452382.704642292,5452212.265970696,5452682.448146508,5452261.282885891,5452731.482010798,5453245.611858933,5453111.008188256,5453160.31490526,5452992.166741708,5453041.184746446,5452870.492327716,5452956.465212219,5452195.090182171,5452244.107094512,5452109.505608898,5452087.7874813,5452099.868719176,5451929.753652407,5451978.771748309,5452380.879895704,5452429.916015471,5452850.790312874,5452901.251938195,5452782.986817012,5452802.448238962,5452684.185304841,5452661.889823016,5452673.952950132,5452503.567284125,5452552.296578999,5451790.649755388,5451804.156430627},
//                {9973.29309055919, 9973.39245805256,9973.397330989092,9973.383986099192,9973.38891858201,9973.371878204334,9973.376779902414,9973.359826356242,9973.357454257546,9973.362384929032,9973.286227238877,9973.291100168366,9973.274285352003,9973.27918715265,9973.26587114058,9973.270772941281},
//                {5453148.210216262,5454141.885149957,5454190.614515289,5454057.16561628,5454106.4904444665,5453936.086667698,5453985.103648492,5453815.568186784,5453791.8471998265,5453841.153914697,5453079.577013142,5453128.306308016,5452960.158144395,5453009.176150855,5452876.016030161,5452925.034037175},
//                {50633949.88139567,50624013.13205872,50623525.8384054,50624860.32739549,50624367.079113625,50626071.11688131,50625580.94707336,50627276.301690444,50627513.511560015,50627020.44441133,50634636.213426866,50634148.92047813,50635830.40211433,50635340.222049735,50636671.82325668,50636181.64318655},
//                {9493.598575277196,9490.116340839557,9489.94557365169,9490.413231973373,9490.240378045479,9490.837541040784,9490.665765933289,9491.259886005302,9491.343013831993,9491.170223380403,9493.83909347421,9493.668326533254,9494.257584957846,9494.085806256011,9494.552452582973,9494.380673879194},
//                {154476.05679147458,154491.5267799328,154492.2854211078,154490.20782726153,154490.97573887615,154488.3228144937,154489.0859333985,154486.44652734432,154486.07722810772,154486.8448577259,154474.98827819715,154475.74691827522,154473.12911031581,154473.89224518865,154471.81914717602,154472.5822820575}
        };

        int total = 0;
        for (double[] numbers: numbersList){
            ByteBuffer encodedBuffer = ByteBuffer.allocate(300000);
            varintCodec.encode(encodedBuffer, numbers, numbers.length);

            encodedBuffer.flip();
            System.out.println(encodedBuffer.remaining());
            total += encodedBuffer.remaining();
            int size = numbers.length;
            System.out.println(size * 8);
            varintCodec.decode(encodedBuffer, Context.getBlockDoubleValues(), size);

            System.out.println(Arrays.toString(Context.getBlockDoubleValues()));
        }

        System.out.println("total:" + total);
    }
}
