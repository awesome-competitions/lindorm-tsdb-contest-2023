package com.alibaba.lindorm.contest.v2.tests;

import java.sql.SQLOutput;

public class TestXOR {

    public static void main(String[] args) {
//        double[] numbers = {15.5, 14.0625, 3.25, 8.625, 13.1};
//        double[] numbers = {9973.29309055919, 9973.39245805256,9973.397330989092,9973.383986099192,9973.38891858201,9973.371878204334,9973.376779902414,9973.359826356242,9973.357454257546,9973.362384929032,9973.286227238877,9973.291100168366,9973.274285352003,9973.27918715265,9973.26587114058,9973.270772941281};
//        double[] numbers = {20155.05092182393,20155.050922867114,20155.050925996657,20155.05093121257,20155.05093851484,20155.050947903474,20155.050959378474,20155.050972939836,20155.05098858756,20155.05100632165,20155.0510261421,20155.051048048914,20155.051072042093,20155.051098121632,20155.051126287533,20155.051156539794};
        double[] numbers = {5453148.210216262,5454141.885149957,5454190.614515289,5454057.16561628,5454106.4904444665,5453936.086667698,5453985.103648492,5453815.568186784,5453791.8471998265,5453841.153914697,5453079.577013142,5453128.306308016,5452960.158144395,5453009.176150855,5452876.016030161,5452925.034037175};
        double preDiff = 0;
        double preDiffDiff = 0;
        for (int i = 0; i < numbers.length; i ++) {
            if (i > 0){
                System.out.println("-----");
                double diff = numbers[i] + - numbers[i - 1] ;
                double diffDiff = diff - preDiff;

//                System.out.println(diff);
//                System.out.println("preDiffDiff: " + preDiffDiff);
//                System.out.println("diffDiff: " + diffDiff);
//                System.out.println("preDiff：" + preDiff);
//                System.out.println("diff：" + diff);
//                long v = Double.doubleToRawLongBits(diffDiff);
//                System.out.println(Long.numberOfLeadingZeros(v));
//                System.out.println(Long.numberOfTrailingZeros(v));

                long v1 = Double.doubleToRawLongBits(numbers[i]);
                long v2 = Double.doubleToRawLongBits(numbers[i - 1]);
                long xorValue = v1 ^ v2;
                int trailingZeros = Long.numberOfTrailingZeros(xorValue);
                System.out.println("trailingZeros: " + trailingZeros);
                System.out.println("xorValue: " + xorValue);
                System.out.println("v:" + (xorValue >> trailingZeros));

                preDiff = diff;
                preDiffDiff = diffDiff;
            }
            System.out.println("====================================");
        }
    }

    public static double abs(double v){
        return v > 0 ? v : -v;
    }

}
