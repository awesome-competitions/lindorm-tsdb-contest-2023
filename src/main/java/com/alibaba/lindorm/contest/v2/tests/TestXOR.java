package com.alibaba.lindorm.contest.v2.tests;

public class TestXOR {

    public static void main(String[] args) {
        double[] numbers = {9973.29309055919, 9973.39245805256,9973.397330989092,9973.383986099192,9973.38891858201,9973.371878204334,9973.376779902414,9973.359826356242,9973.357454257546,9973.362384929032,9973.286227238877,9973.291100168366,9973.274285352003,9973.27918715265,9973.26587114058,9973.270772941281};
        int diffCount = 1;


        double pre2 = numbers[0];
        double pre = numbers[1];
        for (int i = 2; i < numbers.length; i ++){
            double curr = numbers[i];

            long v1 = Double.doubleToLongBits(pre);
            long v2 = Double.doubleToLongBits(curr);
            long v3 = Double.doubleToLongBits(pre2);
            long xor = v1 ^ v2;

            int leadingZeros = Long.numberOfLeadingZeros(xor);
            int trailingZeros = Long.numberOfTrailingZeros(xor);
            System.out.println(leadingZeros  + ":" + trailingZeros + ":" + xor);

            pre2 = pre;
            pre = curr;
        }

    }
}
