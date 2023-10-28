package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;

public class VarIntCodec extends Codec<int[]>{
    @Override
    public void encode(ByteBuffer src, int[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        for (int value : data) {
            encodeVarInt(buffer, value);
        }
        buffer.flip();
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = Context.getBlockIntValues();
        BitBuffer buffer = new DirectBitBuffer(src);
        for (int i = 0; i < size; i++) {
            data[i] = decodeVarInt(buffer);
        }
        return data;
    }

    public static void main(String[] args) {
        VarIntCodec varintCodec = new VarIntCodec();
        int[] numbers = {-13061,-14901,-22085,-13557,-15621,-18085,-16757,-19525,-17285,-15253,-13013,-17045,-20613,-17941,-13285,-19381};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
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
