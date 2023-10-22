package com.alibaba.lindorm.contest.v2.tests;

public class TestXOR {

    public static void main(String[] args) {
        double originalDouble = -2226421.1215246054; // 要压缩的双精度浮点数

        // 将双精度浮点数转换为长整数，以便进行XOR操作
        long longValue = Double.doubleToRawLongBits(originalDouble);

        // 创建一个密钥，用于XOR操作
        long xorKey = Double.doubleToLongBits(-2226421.152820058);

        // 进行XOR操作
        long compressedValue = longValue ^ xorKey;

        // 将压缩后的值再次解压缩为双精度浮点数
        long decompressedValue = compressedValue ^ xorKey;
        double decompressedDouble = Double.longBitsToDouble(decompressedValue);

        System.out.println("原始双精度浮点数: " + originalDouble);
        System.out.println("压缩后的长整数: " + compressedValue);
        System.out.println("解压缩后的双精度浮点数: " + decompressedDouble);
    }
}
