package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;

public class StringHuffman3Codec extends Codec<ByteBuffer[]>{

    private Huffman3Codec codec;

    public StringHuffman3Codec(int min) {
        this.codec = new Huffman3Codec(min);
    }

    @Override
    public void encode(ByteBuffer src, ByteBuffer[] data, int size) {
        int[] intData = new int[size];
        for (int i = 0; i < size; i++) {
            ByteBuffer buffer = data[i];
            if (buffer.remaining() == 1){
                intData[i] = buffer.get() - '0';
            }else{
                intData[i] = -1;
            }
        }
        codec.encode(src, intData, size);
    }

    @Override
    public void decode(ByteBuffer src, ByteBuffer[] data, int size) {
        int[] intData = new int[size];
        codec.decode(src, intData, size);
        for (int i = 0; i < size; i++) {
            if (intData[i] == -1){
                data[i] = ByteBuffer.wrap(new byte[]{'-', '1'});
            }else{
                data[i] = ByteBuffer.wrap(new byte[]{(byte) (intData[i] + '0')});
            }
        }
    }

    public static void main(String[] args) {
        StringHuffman3Codec bc = new StringHuffman3Codec(-1);

        String[] strings = new String[]{
                "-1",
                "0",
                "1",
                "-1"
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
