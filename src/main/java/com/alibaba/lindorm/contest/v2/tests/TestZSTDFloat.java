package com.alibaba.lindorm.contest.v2.tests;

import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestZSTDFloat {

    public static void main(String[] args) throws IOException {

        double[] numbers = {9973.39245805256,9973.397330989092,9973.383986099192,9973.38891858201,9973.371878204334,9973.376779902414,9973.359826356242,9973.357454257546,9973.362384929032,9973.286227238877,9973.291100168366,9973.274285352003,9973.27918715265,9973.26587114058,9973.270772941281};

        ByteBuffer data = ByteBuffer.allocateDirect(numbers.length * 8);
        for (double number : numbers) {
            data.putDouble(number);
        }
        data.flip();




        // 压缩数据
        ByteBuffer dst = Zstd.compress(data, 3);
        int compressedSize = dst.remaining();

        // 解压数据
        ByteBuffer decompressed = ByteBuffer.allocateDirect(numbers.length * 8);
        long size = Zstd.decompress(decompressed, dst);

        System.out.println(numbers.length * 8);
        System.out.println(compressedSize);

    }
}
