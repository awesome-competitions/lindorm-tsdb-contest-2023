package com.alibaba.lindorm.contest.util;

import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class Deflate {

    public static void compress(ByteBuffer dst, ByteBuffer src) {
        try {
            Deflater deflater = new Deflater();
            deflater.setInput(src);
            deflater.finish();
            while (!deflater.finished()) {
                deflater.deflate(dst);
            }
            deflater.end();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void decompress(ByteBuffer dst, ByteBuffer src) {
        try {
            Inflater inflater = new Inflater();
            inflater.setInput(src);
            while (!inflater.finished()) {
                inflater.inflate(dst);
            }
            inflater.end();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String originalString = "This is a test string for compression and decompression using Deflate algorithm.";
        byte[] originalBytes = originalString.getBytes();
        ByteBuffer srcBuffer = ByteBuffer.allocateDirect(originalBytes.length);
        srcBuffer.put(originalBytes);
        srcBuffer.flip();
        ByteBuffer compressedBuffer = ByteBuffer.allocateDirect(originalBytes.length);
        ByteBuffer decompressedBuffer = ByteBuffer.allocateDirect(originalBytes.length);

        // 压缩
        compress(compressedBuffer, srcBuffer);
        compressedBuffer.flip();

        // 解压缩
        decompress(decompressedBuffer, compressedBuffer);
        decompressedBuffer.flip();

        // 将解压缩后的字节转换回字符串并输出
        byte[] decompressedBytes = new byte[decompressedBuffer.remaining()];
        decompressedBuffer.get(decompressedBytes);
        String decompressedString = new String(decompressedBytes);
        System.out.println("Decompressed String: " + decompressedString);
    }
}
