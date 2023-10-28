package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaOfDeltaIntCodec extends Codec<int[]> {

    private final int deltaSize;

    private final int deltaSizeBits;

    public DeltaOfDeltaIntCodec(int deltaSize) {
        this.deltaSize = deltaSize;
        this.deltaSizeBits = Util.parseBits(deltaSize, false);
    }

    public DeltaOfDeltaIntCodec(int deltaSize, boolean unsigned) {
        this.deltaSize = deltaSize;
        this.deltaSizeBits = Util.parseBits(deltaSize, unsigned);
    }

    @Override
    public void encode(ByteBuffer src, int[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        encodeVarInt(buffer, data[0]);
        if (data.length > 1){
            encodeVarInt(buffer, data[1] - data[0]);
            int preDiff = data[1] - data[0];
            for (int i = 2; i < data.length; i++) {
                int diff = data[i] - data[i - 1];
                if (Math.abs(diff - preDiff) > deltaSize) {
                    throw new RuntimeException("delta size is too small," + deltaSize + " < " + Math.abs(diff - preDiff) + " at " + i + "th");
                }
                buffer.putInt(diff - preDiff, deltaSizeBits);
                preDiff = diff;
            }
        }
        buffer.flip();
    }

    @Override
    public int[] decode(ByteBuffer src, int size) {
        int[] data = Context.getBlockIntValues();
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = decodeVarInt(buffer);
        if (size > 1){
            data[1] = data[0] + decodeVarInt(buffer);
            int preDiff = data[1] - data[0];
            for (int i = 2; i < size; i++) {
                int diff = buffer.getInt(deltaSizeBits) + preDiff;
                data[i] = data[i - 1] + diff;
                preDiff = diff;
            }
        }
        return data;
    }

    public static void main(String[] args) {
//        Codec<int[]> compressor = new DeltaOfDeltaIntCodec(3, true);
//        ByteBuffer src = ByteBuffer.allocate(1000);
//        int [] values = new int[]{-127482, -127484, -127485, -127487, -127489, -127491, -127493, -127495, -127497, -127499, -127500, -127502, -127504, -127506, -127508, -127510, -127512, -127514, -127515, -127517, -127519, -127521, -127523, -127525, -127527, -127529, -127531, -127533, -127534, -127536, -127538, -127540, -127542, -127544, -127546, -127548, -127550, -127552, -127553, -127555, -127557, -127559, -127561, -127563, -127565, -127567, -127569, -127571, -127573, -127575, -127576, -127578, -127580, -127582, -127584, -127586, -127588, -127590, -127592, -127594, -127596, -127598, -127600, -127602, -127603, -127605, -127607, -127609, -127611, -127613, -127615, -127617, -127619, -127621, -127623, -127625, -127627, -127629, -127631, -127633, -127635, -127637, -127638, -127640, -127642, -127644, -127646, -127648, -127650, -127652, -127654, -127656, -127658, -127660, -127662, -127664, -127666, -127668, -127670, -127672, -127674, -127676, -127678, -127680, -127682, -127684, -127686, -127688, -127690, -127692, -127694, -127696, -127698, -127700, -127702, -127704, -127706, -127708, -127710, -127712, -127714, -127716, -127718, -127720, -127722, -127724, -127725, -127728, -127730, -127732, -127734, -127736, -127738, -127740, -127742, -127744, -127746, -127748, -127750, -127752, -127754, -127756, -127758, -127760, -127762, -127764, -127766, -127768, -127770, -127772, -127774, -127776, -127778, -127780, -127782, -127784, -127786, -127788, -127790, -127792, -127794, -127796, -127798, -127800, -127802, -127804, -127806, -127808, -127810, -127812, -127815, -127817, -127819, -127821, -127823, -127825, -127827, -127829, -127831, -127833, -127835, -127837, -127839, -127841, -127843, -127845, -127847, -127849, -127851, -127854, -127856, -127858, -127860, -127862, -127864, -127866, -127868, -127870, -127872, -127874};
//        compressor.encode(src, values);
//        src.flip();
//
//        System.out.println(src.limit());
//        System.out.println(values.length);
//
//        int[] data = compressor.decode(src, values.length);
//        System.out.println(Arrays.toString(data));

        BitBuffer bf = new ArrayBitBuffer(13213);
        bf.put(2, 2);
        bf.flip();
        System.out.println(bf.getIntUnsigned(2));
    }


}
