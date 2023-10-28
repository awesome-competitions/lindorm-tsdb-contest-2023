package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaOfDeltaVarIntCodec extends Codec<int[]> {

    public DeltaOfDeltaVarIntCodec() {
    }

    @Override
    public void encode(ByteBuffer src, int[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putInt(data[0]);
        if (data.length > 1){
            buffer.putInt(data[1]);
            int preDiff = data[1] - data[0];
            for (int i = 2; i < data.length; i++) {
                int diff = data[i] - data[i - 1];
                encodeVarInt(buffer, diff - preDiff);
                preDiff = diff;
            }
        }
        buffer.flip();
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = Context.getBlockIntValues();
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = buffer.getInt();
        if (size > 1){
            data[1] = buffer.getInt();
            int preDiff = data[1] - data[0];
            for (int i = 2; i < size; i++) {
                int diff = decodeVarInt(buffer) + preDiff;
                data[i] = data[i - 1] + diff;
                preDiff = diff;
            }
        }
        return data;
    }

    public static void main(String[] args) {
        DeltaOfDeltaVarIntCodec varintCodec = new DeltaOfDeltaVarIntCodec();
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
