package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;

public class StringHuffman3Codec extends Codec<ByteBuffer[]>{

    private final Huffman3Codec codec;

    private final static ByteBuffer MINUS_ONE = ByteBuffer.wrap(new byte[]{'-', '1'});
    private final static ByteBuffer ZERO = ByteBuffer.wrap(new byte[]{'0'});
    private final static ByteBuffer ONE = ByteBuffer.wrap(new byte[]{'1'});

    public StringHuffman3Codec(int min) {
        this.codec = new Huffman3Codec(min);
    }

    @Override
    public void encode(ByteBuffer src, ByteBuffer[] data, int size) {
        int[] intData = new int[size];
        for (int i = 0; i < size; i++) {
            ByteBuffer buffer = data[i];
            if (buffer.remaining() > 0){
                if (buffer.get(0) == '-'){
                    intData[i] = -1;
                }else if (buffer.get(0) == '0'){
                    intData[i] = 0;
                }else if (buffer.get(0) == '1'){
                    intData[i] = 1;
                }else{
                    throw new RuntimeException("invalid value " + new String(buffer.array()));
                }
            }
        }
        codec.encode(src, intData, size);
    }

    @Override
    public void decode(ByteBuffer src, ByteBuffer[] data, int size) {
        int[] intData = new int[size];
        codec.decode(src, intData, size);
        for (int i = 0; i < size; i++) {
            if (intData[i] == -1) {
                data[i] = MINUS_ONE;
            }else if (intData[i] == 0) {
                data[i] = ZERO;
            } else if (intData[i] == 1) {
                data[i] = ONE;
            } else{
                throw new RuntimeException("invalid value " + intData[i]);
            }
        }
    }

    public static void main(String[] args) {
        StringHuffman3Codec bc = new StringHuffman3Codec(-1);

        String[] strings = new String[]{
                "0",
                "0",
                "0",
                "0",
                "0",
                "-1",
                "0",
                "1",
                "-1",
                "1",
                "1",
                "0",
                "-1",
                "-1",
                "-1",
                "-1",
                "-1",
                "-1",
                "-1",
                "-1",
                "-1",
                "-1",
        };

        ByteBuffer[] buffers = new ByteBuffer[strings.length];
        for (int i = 0; i < strings.length; i++) {
            buffers[i] = ByteBuffer.wrap(strings[i].getBytes());
        }

        ByteBuffer src = ByteBuffer.allocate(1024);
        bc.encode(src, buffers, buffers.length);
        src.flip();

        ByteBuffer[] dst = new ByteBuffer[strings.length];
        bc.decode(src, dst, dst.length);
        for (int i = 0; i < dst.length; i++) {
            System.out.println(new String(dst[i].array()));
        }


    }
}
