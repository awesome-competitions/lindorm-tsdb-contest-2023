package com.alibaba.lindorm.contest.v2.codec;

import java.nio.ByteBuffer;

public class VarIntCodec extends Codec<Integer>{
    @Override
    public void encode(ByteBuffer src, Integer[] data) {
        for (int value : data) {
            encodeVarInt(src, value);
        }
    }

    @Override
    public Integer[] decode(ByteBuffer src, int size) {
        Integer[] data = new Integer[size];
        for (int i = 0; i < size; i++) {
            data[i] = decodeVarInt(src);
        }
        return data;
    }

    public static void main(String[] args) {
        VarIntCodec varintCodec = new VarIntCodec();
        Integer[] numbers = {1,2,3,4,5,6,-7,8,9,10};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(64);
        varintCodec.encode(encodedBuffer, numbers);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());

        int size = numbers.length;
        Integer[] decodedNumbers = varintCodec.decode(encodedBuffer, size);

        for (Integer num : decodedNumbers) {
            System.out.println("Decoded: " + num);
        }
    }
}
