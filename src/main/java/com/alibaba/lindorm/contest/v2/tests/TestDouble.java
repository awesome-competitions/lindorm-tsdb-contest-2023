package com.alibaba.lindorm.contest.v2.tests;

import com.alibaba.lindorm.contest.v2.Context;
import com.alibaba.lindorm.contest.v2.codec.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class TestDouble {

    public static void main(String[] args) {

        double[] numbers = new double[]{5453214.744962816,5453044.359296671,5453057.866008186,5452959.062308822,5452974.301314467,5453466.793735752,5453444.211729925,5453345.697835388,5453323.148604929,5453224.076427234,5453200.337395937,5453178.044031174,5453079.240402496,5453631.714510954,5452942.498935414,5452956.871740541,5452858.3568196  ,5452835.792791409,5452700.901503362,5452749.91732521 ,5452579.515799924,5452591.883494316,5452569.301490623,5453062.641997589,5453040.059993247,5452941.274338493,5452991.735961638,5453394.132821335,5453442.862184492,5453274.715047356,5453324.019645315,5453264.482690508,5452574.400985392,5452552.703085199,5452453.899456385,5452467.406131527,5452297.002421041,5452382.704642292,5452212.265970696,5452682.448146508,5452261.282885891,5452731.482010798,5453245.611858933,5453111.008188256,5453160.31490526,5452992.166741708,5453041.184746446,5452870.492327716,5452956.465212219,5452195.090182171,5452244.107094512,5452109.505608898,5452087.7874813,5452099.868719176,5451929.753652407,5451978.771748309,5452380.879895704,5452429.916015471,5452850.790312874,5452901.251938195,5452782.986817012,5452802.448238962,5452684.185304841,5452661.889823016,5452673.952950132,5452503.567284125,5452552.296578999,5451790.649755388,5451804.156430627};
        int total = 0;

        double[] diffs = new double[numbers.length - 1];
        for (int i = 1; i < numbers.length; i ++){
            double v = numbers[i] - numbers[i-1];
            diffs[i-1] = v;
            long v1 = Double.doubleToLongBits(numbers[i]);
            long v2 = Double.doubleToLongBits(numbers[i-1]);
            long xor = v1 ^ v2;

            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);
            System.out.println(leadingZeros + " " + trailingZeros + " " + (64 - leadingZeros - trailingZeros) + " " + xor);

            total += (64 - leadingZeros - trailingZeros);
        }

        System.out.println(total / 8);
        System.out.println(numbers.length * 64 / 8);

//        Random random = new Random();
//        List<Double> list = new ArrayList<>();
//        for (double d: numbers){
//            list.add(d);
//        }
//        double pre = list.get(list.size() - 1);
//        for (int i = 0; i < 600 - numbers.length; i ++){
//            double v = pre + random.nextDouble() + random.nextInt(100);
//            list.add(v);
//            pre = v;
//        }
//        for (int i = 1; i < 600; i ++){
//            System.out.println(list.get(i) - list.get(i-1));
//        }
    }
}
