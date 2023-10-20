package com.alibaba.lindorm.contest.v2.codec;

import java.nio.ByteBuffer;

public class VarIntCodec extends Codec<int[]>{
    @Override
    public void encode(ByteBuffer src, int[] data) {
        for (int value : data) {
            encodeVarInt(src, value);
        }
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = new int[size];
        for (int i = 0; i < size; i++) {
            data[i] = decodeVarInt(src);
        }
        return data;
    }

    public static void main(String[] args) {
        VarIntCodec varintCodec = new VarIntCodec();
        int[] numbers = {1,2,3,4,5,6,-7,8,9,10};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(64);
        varintCodec.encode(encodedBuffer, numbers);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());

        int size = numbers.length;
        int[] decodedNumbers = varintCodec.decode(encodedBuffer, size);

        for (Integer num : decodedNumbers) {
            System.out.println("Decoded: " + num);
        }
    }
}
