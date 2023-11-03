package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaOfDeltaIntCodec extends Codec<int[]> {

    private static final int MAX_RLE = 8;
    private static final int RLE_BITS = 3;

    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        encodeVarInt(buffer, data[0]);
        if (data.length > 1){
            int preDiff = data[1] - data[0];
            encodeVarInt(buffer, preDiff);
            int maxDelta = Integer.MIN_VALUE;
            for (int i = 2; i < size; i++) {
                int diff = data[i] - data[i - 1];
                int v = encodeZigzag(diff - preDiff);
                if (v > maxDelta){
                    maxDelta = v;
                }
                preDiff = diff;
            }

            int deltaBits = Util.parseBits(maxDelta, true);
            buffer.putInt(deltaBits, 2);
            preDiff = data[1] - data[0];
            int rl = 0;
            for (int i = 2; i < size; i++) {
                int diff = data[i] - data[i - 1];
                int v = encodeZigzag(diff - preDiff);
                if (rl == MAX_RLE || (v != 0 && rl > 0)) {
                    buffer.putInt(rl - 1, RLE_BITS);
                    rl = 0;
                }
                if (v == 0){
                    if (rl == 0){
                        buffer.putInt(v, deltaBits);
                    }
                    rl ++;
                }else{
                    buffer.putInt(v, deltaBits);
                }
                preDiff = diff;
            }
            if (rl > 1){
                buffer.putInt(rl - 1, RLE_BITS);
            }
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = decodeVarInt(buffer);
        if (size > 1){
            data[1] = data[0] + decodeVarInt(buffer);
            int deltaBits = buffer.getIntUnsigned(2);
            int preDiff = data[1] - data[0];
            int i = 2;
            while (i < size){
                int v = decodeZigzag(buffer.getIntUnsigned(deltaBits));
                int diff = v + preDiff;
                data[i] = data[i-1] + diff;
                i ++;
                if (i == size){
                    break;
                }
                if (v == 0) {
                    int rl = buffer.getIntUnsigned(RLE_BITS);
                    for (int j = 0; j < rl; j ++) {
                        data[i] = data[i - 1] + diff;
                        i ++;
                    }
                }
                preDiff = diff;
            }
        }
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new DeltaOfDeltaIntCodec();
        ByteBuffer src = ByteBuffer.allocate(1000);
//        int [] values = new int[]{1153530, 1153530, 1153530, 1153530, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153532, 1153532, 1153532, 1153532, 1153532, 1153533, 1153533, 1153533, 1153533, 1153533, 1153534, 1153534, 1153534, 1153535, 1153535, 1153535, 1153535, 1153536, 1153536, 1153536, 1153537, 1153537, 1153538, 1153538, 1153538, 1153539, 1153539, 1153540, 1153540, 1153541, 1153541, 1153541, 1153542, 1153542, 1153543, 1153543, 1153544, 1153545, 1153545, 1153546, 1153546, 1153547, 1153547, 1153548, 1153549, 1153549, 1153550, 1153550, 1153551, 1153552, 1153552, 1153553, 1153554, 1153555, 1153555, 1153556, 1153557, 1153557, 1153558, 1153559, 1153560, 1153561, 1153561, 1153562, 1153563, 1153564, 1153565, 1153565, 1153566, 1153567, 1153568, 1153569, 1153570, 1153571, 1153572, 1153573, 1153574, 1153575, 1153576, 1153577, 1153578, 1153578, 1153580, 1153581, 1153582, 1153583, 1153584, 1153585, 1153586, 1153587, 1153588, 1153589, 1153590, 1153591, 1153592, 1153594, 1153595, 1153596, 1153597, 1153598, 1153599, 1153601, 1153602, 1153603, 1153604, 1153606, 1153607, 1153608, 1153609, 1153611, 1153612, 1153613, 1153615, 1153616, 1153617, 1153619, 1153620, 1153621, 1153623, 1153624, 1153625, 1153627, 1153628, 1153630, 1153631, 1153633, 1153634, 1153636, 1153637, 1153639, 1153640, 1153642, 1153643, 1153645, 1153646, 1153648, 1153649, 1153651, 1153653, 1153654, 1153656, 1153657, 1153659, 1153661, 1153662, 1153664, 1153666, 1153667, 1153669, 1153671, 1153672, 1153674, 1153676, 1153678, 1153679, 1153681, 1153683, 1153685, 1153687, 1153688, 1153690, 1153692, 1153694, 1153696, 1153698, 1153699, 1153701, 1153703, 1153705, 1153707, 1153709, 1153711, 1153713, 1153715, 1153717, 1153719, 1153721, 1153723, 1153725, 1153727, 1153729, 1153731, 1153733, 1153735, 1153737};
        int [] values = new int[]{1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
        compressor.encode(src, values, values.length);
        src.flip();

        System.out.println(src.limit());
        System.out.println(values.length*4);

        compressor.decode(src, Context.getBlockIntValues(), values.length);
        System.out.println(Arrays.toString(Context.getBlockIntValues()));
    }


}
