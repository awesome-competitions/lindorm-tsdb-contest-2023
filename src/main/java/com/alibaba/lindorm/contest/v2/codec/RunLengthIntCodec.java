package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class RunLengthIntCodec extends Codec<int[]>{

    private final int runLengthMaxSize;

    private final int runLengthSizeBits;

    public RunLengthIntCodec(int runLengthMaxSize) {
        this.runLengthMaxSize = runLengthMaxSize;
        this.runLengthSizeBits = Util.parseBits(runLengthMaxSize, false);
    }

    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        int v = data[0];
        int len = 1;
        for (int i = 1; i < size; i++) {
           if (data[i] == v && len < runLengthMaxSize){
               len ++;
               continue;
           }
           encodeVarInt(buffer, v);
           buffer.putInt(len, runLengthSizeBits);
           v = data[i];
           len = 1;
        }
        encodeVarInt(buffer, v);
        buffer.putInt(len, runLengthSizeBits);
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        int index = 0;
        while (index < size){
            int v = decodeVarInt(buffer);
            int len = buffer.getInt(runLengthSizeBits);
            for (int i = 0; i < len; i++) {
                data[index++] = v;
            }
        }
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new RunLengthIntCodec(15);
        ByteBuffer src = ByteBuffer.allocate(1000);
        int[] ints = new int[]{12393,12393,12393,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394,12394};
        compressor.encode(src, ints, ints.length);
        src.flip();
        System.out.println(src.remaining());
        compressor.decode(src, Context.getBlockIntValues(), ints.length);
        System.out.println(Arrays.toString(Context.getBlockIntValues()));
    }

}
