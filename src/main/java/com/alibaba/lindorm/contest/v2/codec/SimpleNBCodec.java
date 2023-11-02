package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class SimpleNBCodec extends Codec<int[]>{

    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        int minValue = Integer.MAX_VALUE;
        int maxValue = Integer.MIN_VALUE;
        for (int i = 0; i < size; i++) {
            if (data[i] < minValue){
                minValue = data[i];
            }
            if (data[i] > maxValue){
                maxValue = data[i];
            }
        }
        int maxBits = Util.parseBits(maxValue - minValue, true);
        BitBuffer buffer = new DirectBitBuffer(src);
        encodeVarInt(buffer, minValue);
        buffer.put(maxBits, 5);
        for (int value : data) {
            buffer.putInt(value - minValue, maxBits);
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        int min = decodeVarInt(buffer);
        int bits = buffer.getIntUnsigned(5);
        for (int i = 0; i < size; i++) {
            data[i] = buffer.getIntUnsigned(bits) + min;
        }
    }

    public static void main(String[] args) {
        SimpleNBCodec varintCodec = new SimpleNBCodec();
//        int[] numbers = {47508323,38447491,9735651,48158467,31176995,21444931,30686115,41353795,3464675,12283267,16355299,22657539,37193507,44959939,46392611,4670915};
        int[] numbers = {-16549,-16293,-18901,-20757,-13317,-19189,-14981,-12741,-17461,-19749,-19861,-20549,-22533,-18981,-21877,-19877,-19029,-21317,-19893,-21877,-22565,-14789,-16101,-13525,-20357,-13221,-20885,-22245,-19909,-19141,-12885,-21813,-19813,-13253,-22213,-22277,-19365,-20709,-15237,-18469,-21765,-21333,-20917,-20309,-19941,-20485,-21253,-21381,-18181,-12821,-22117,-22085,-21141,-20517,-22581,-19445,-15669,-20885,-18069,-19909,-18005,-17093,-20645,-18901,-17573,-20341,-18101,-20165,-20949};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
        varintCodec.encode(encodedBuffer, numbers, numbers.length);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());
        System.out.println(numbers.length * 4);

        int size = numbers.length;
        varintCodec.decode(encodedBuffer, Context.getBlockIntValues(), size);
        System.out.println(Arrays.toString(Context.getBlockIntValues()));
    }
}
