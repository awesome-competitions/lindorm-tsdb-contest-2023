package com.alibaba.lindorm.contest.v2.tests;

public class TestXOR {

    public static void main(String[] args) {
        double[] numbers = {15.5, 14.0625, 3.25, 8.625, 13.1};

        for (int i = 0; i < numbers.length; i++) {
            System.out.println(Long.toHexString(Double.doubleToLongBits(numbers[i])));
            if (i > 0){
                long xorValue = Double.doubleToLongBits(numbers[i]) ^ Double.doubleToLongBits(numbers[i - 1]);
                System.out.println(xorValue);
                System.out.println(Long.toHexString(xorValue));
                System.out.println(Long.numberOfLeadingZeros(xorValue));
                System.out.println(Long.numberOfTrailingZeros(xorValue));
                System.out.println(xorValue >> Long.numberOfTrailingZeros(xorValue));
                System.out.println((xorValue >> Long.numberOfTrailingZeros(xorValue)) << Long.numberOfTrailingZeros(xorValue));
                System.out.println("====================================");
            }
        }
    }
}
