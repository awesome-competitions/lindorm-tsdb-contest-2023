package com.alibaba.lindorm.contest.v2.tests;

import java.sql.SQLOutput;

public class TestXOR {

    public static void main(String[] args) {
//        double[] numbers = {15.5, 14.0625, 3.25, 8.625, 13.1};
        double[] numbers = {20155.050922867114,20155.050925996657,20155.05093121257,20155.05093851484,20155.050947903474,20155.050959378474,20155.050972939836,20155.05098858756,20155.05100632165,20155.0510261421,20155.051048048914,20155.051072042093,20155.051098121632,20155.051126287533,20155.051156539794};
        double preDiff = 0;
        for (int i = 0; i < numbers.length; i++) {
            System.out.println(Long.toHexString(Double.doubleToLongBits(numbers[i])));
            if (i > 0){
                System.out.println("-----");
                double diff = numbers[i] - numbers[i - 1];
                long xorValue = Double.doubleToLongBits(diff) ^ Double.doubleToLongBits(preDiff);
                System.out.println(xorValue);
                System.out.println(Long.numberOfLeadingZeros(xorValue));
                System.out.println(Long.numberOfTrailingZeros(xorValue));
                System.out.println(xorValue >> Long.numberOfTrailingZeros(xorValue));
                preDiff = diff;
            }
            System.out.println("====================================");
        }
    }
}
