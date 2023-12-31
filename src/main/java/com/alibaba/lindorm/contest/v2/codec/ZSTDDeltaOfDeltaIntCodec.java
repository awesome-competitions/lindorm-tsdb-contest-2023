package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ZSTDDeltaOfDeltaIntCodec extends Codec<int[]>{

    private final DeltaOfDeltaIntCodec codec = new DeltaOfDeltaIntCodec();

    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        ByteBuffer encodeBuffer = Context.getCodecEncodeBuffer().clear();
        codec.encode(encodeBuffer, data, size);
        encodeBuffer.flip();
        Zstd.compress(src, encodeBuffer);
    }

    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        ByteBuffer decodeBuffer = Context.getCodecDecodeBuffer().clear();
        Zstd.decompress(decodeBuffer, src);
        decodeBuffer.flip();
        codec.decode(decodeBuffer, data, size);
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new ZSTDDeltaOfDeltaIntCodec();
        ByteBuffer src = ByteBuffer.allocateDirect(1000);
        int [] values = new int[]{1153530, 1153530, 1153530, 1153530, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153532, 1153532, 1153532, 1153532, 1153532, 1153533, 1153533, 1153533, 1153533, 1153533, 1153534, 1153534, 1153534, 1153535, 1153535, 1153535, 1153535, 1153536, 1153536, 1153536, 1153537, 1153537, 1153538, 1153538, 1153538, 1153539, 1153539, 1153540, 1153540, 1153541, 1153541, 1153541, 1153542, 1153542, 1153543, 1153543, 1153544, 1153545, 1153545, 1153546, 1153546, 1153547, 1153547, 1153548, 1153549, 1153549, 1153550, 1153550, 1153551, 1153552, 1153552, 1153553, 1153554, 1153555, 1153555, 1153556, 1153557, 1153557, 1153558, 1153559, 1153560, 1153561, 1153561, 1153562, 1153563, 1153564, 1153565, 1153565, 1153566, 1153567, 1153568, 1153569, 1153570, 1153571, 1153572, 1153573, 1153574, 1153575, 1153576, 1153577, 1153578, 1153578, 1153580, 1153581, 1153582, 1153583, 1153584, 1153585, 1153586, 1153587, 1153588, 1153589, 1153590, 1153591, 1153592, 1153594, 1153595, 1153596, 1153597, 1153598, 1153599, 1153601, 1153602, 1153603, 1153604, 1153606, 1153607, 1153608, 1153609, 1153611, 1153612, 1153613, 1153615, 1153616, 1153617, 1153619, 1153620, 1153621, 1153623, 1153624, 1153625, 1153627, 1153628, 1153630, 1153631, 1153633, 1153634, 1153636, 1153637, 1153639, 1153640, 1153642, 1153643, 1153645, 1153646, 1153648, 1153649, 1153651, 1153653, 1153654, 1153656, 1153657, 1153659, 1153661, 1153662, 1153664, 1153666, 1153667, 1153669, 1153671, 1153672, 1153674, 1153676, 1153678, 1153679, 1153681, 1153683, 1153685, 1153687, 1153688, 1153690, 1153692, 1153694, 1153696, 1153698, 1153699, 1153701, 1153703, 1153705, 1153707, 1153709, 1153711, 1153713, 1153715, 1153717, 1153719, 1153721, 1153723, 1153725, 1153727, 1153729, 1153731, 1153733, 1153735, 1153737};
        compressor.encode(src, values, values.length);
        src.flip();

        System.out.println(src.limit());
        System.out.println(values.length*4);

        compressor.decode(src, Context.getBlockIntValues(), values.length);
        System.out.println(Arrays.toString(Context.getBlockIntValues()));
    }
}
