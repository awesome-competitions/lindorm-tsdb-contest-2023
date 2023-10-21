package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaOfDeltaIntCodec extends Codec<int[]>{

    private final int deltaSize;

    private final int deltaSizeBits;

    public DeltaOfDeltaIntCodec(int deltaSize) {
        this.deltaSize = deltaSize;
        this.deltaSizeBits = Util.parseBits(deltaSize, false);
    }

    @Override
    public void encode(ByteBuffer src, int[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putInt(data[0]);
        int preDiff = 0;
        for (int i = 1; i < data.length; i++) {
            int diff = data[i] - data[i - 1];
            if (Math.abs(diff - preDiff) > deltaSize ){
                throw new RuntimeException("delta size is too small," + deltaSize + " < " + Math.abs(diff - preDiff) + " at " + i + "th");
            }
            buffer.putInt(diff - preDiff, deltaSizeBits);
            preDiff = diff;
        }
        buffer.flip();
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = new int[size];
        BitBuffer buffer = new DirectBitBuffer(src);
        int v = buffer.getInt();
        data[0] = v;
        int preDiff = 0;
        for (int i = 1; i < size; i++) {
            int diff = buffer.getInt(deltaSizeBits) + preDiff;
            data[i] = data[i-1] + diff;
            preDiff = diff;
        }
        return data;
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new DeltaOfDeltaIntCodec(7);
        ByteBuffer src = ByteBuffer.allocate(1000);
        int [] values = new int[]{-18057547, -18057548, -18057548, -18057548, -18057549, -18057550, -18057550, -18057552, -18057553, -18057554, -18057556, -18057558, -18057559, -18057562, -18057564, -18057566, -18057569, -18057572, -18057574, -18057581, -18057578, -18057584, -18057588, -18057592, -18057600, -18057595, -18057604, -18057608, -18057613, -18057618, -18057623, -18057628, -18057633, -18057638, -18057644, -18057650, -18057656, -18057662, -18057668, -18057674, -18057681, -18057688, -18057695, -18057702, -18057709, -18057716, -18057724, -18057732, -18057740, -18057748, -18057756, -18057764, -18057773, -18057782, -18057791, -18057800, -18057809, -18057819, -18057828, -18057838, -18057848, -18057858, -18057868, -18057879, -18057889, -18057900, -18057911, -18057922, -18057933, -18057945, -18057956, -18057968, -18057980, -18057992, -18058004, -18058017, -18058029, -18058042, -18058055, -18058068, -18058082, -18058095, -18058109, -18058122, -18058136, -18058150, -18058165, -18058179, -18058194, -18058208, -18058223, -18058239, -18058254, -18058269, -18058285, -18058301, -18058317, -18058333, -18058349, -18058365, -18058382, -18058399, -18058416, -18058433, -18058450, -18058468, -18058485, -18058503, -18058521, -18058539, -18058557, -18058576, -18058594, -18058613, -18058632, -18058651, -18058670, -18058690, -18058709, -18058729, -18058749, -18058769, -18058790, -18058810, -18058831, -18058851, -18058872, -18058893, -18058915, -18058936, -18058958, -18058980, -18059002, -18059024, -18059046, -18059068, -18059091, -18059114, -18059137, -18059160, -18059183, -18059207, -18059230, -18059254, -18059278, -18059302, -18059326, -18059351, -18059375, -18059400, -18059425, -18059450, -18059476, -18059501, -18059527, -18059552, -18059578, -18059604, -18059631, -18059657, -18059684, -18059711, -18059738, -18059765, -18059792, -18059819, -18059847, -18059875, -18059903, -18059931, -18059959, -18059988, -18060016, -18060045, -18060074, -18060103, -18060133, -18060162, -18060192, -18060221, -18060251, -18060281, -18060312, -18060342, -18060373, -18060404, -18060435, -18060466, -18060497, -18060528, -18060560, -18060592, -18060624, -18060656, -18060688, -18060721, -18060753, -18060786, -18060819, -18060852};
        compressor.encode(src, values);
        src.flip();
        int[] data = compressor.decode(src, values.length);
        System.out.println(Arrays.toString(data));
    }
}
