package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
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

    @Override
    public void encode(ByteBuffer src, int[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putInt(data[0]);
        if (data.length > 1){
            buffer.putInt(data[1]);
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
        int[] data = new int[size];
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = buffer.getInt();
        if (size > 1){
            data[1] = buffer.getInt();
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
        Codec<int[]> compressor = new DeltaOfDeltaIntCodec(3);
        ByteBuffer src = ByteBuffer.allocate(1000);
        int [] values = new int[]{2254807, 2254811, 2254815, 2254819, 2254824, 2254828, 2254832, 2254836, 2254841, 2254845, 2254850, 2254854, 2254858, 2254863, 2254867, 2254872, 2254876, 2254881, 2254885, 2254890, 2254894, 2254899, 2254904, 2254908, 2254913, 2254918, 2254922, 2254927, 2254932, 2254937, 2254941, 2254946, 2254951, 2254956, 2254961, 2254966, 2254970, 2254975, 2254980, 2254985, 2254990, 2254995, 2255000, 2255005, 2255011, 2255016, 2255021, 2255026, 2255031, 2255036, 2255041, 2255047, 2255052, 2255057, 2255062, 2255068, 2255073, 2255078, 2255084, 2255089, 2255095, 2255100, 2255106, 2255111, 2255117, 2255122, 2255128, 2255133, 2255139, 2255144, 2255150, 2255156, 2255161, 2255167, 2255173, 2255178, 2255184, 2255190, 2255196, 2255201, 2255207, 2255213, 2255219, 2255225, 2255231, 2255237, 2255243, 2255249, 2255255, 2255261, 2255267, 2255273, 2255279, 2255285, 2255291, 2255297, 2255303, 2255310, 2255316, 2255322, 2255328, 2255335, 2255341, 2255347, 2255354, 2255360, 2255366, 2255373, 2255379, 2255385, 2255392, 2255398, 2255405, 2255411, 2255418, 2255425, 2255431, 2255438, 2255444, 2255451, 2255458, 2255464, 2255471, 2255478, 2255485, 2255491, 2255498, 2255505, 2255512, 2255519, 2255525, 2255532, 2255539, 2255546, 2255553, 2255560, 2255567, 2255574, 2255581, 2255588, 2255595, 2255602, 2255610, 2255617, 2255624, 2255631, 2255638, 2255645, 2255653, 2255660, 2255667, 2255675, 2255682, 2255689, 2255697, 2255704, 2255711, 2255719, 2255726, 2255734, 2255741, 2255749, 2255756, 2255764, 2255772, 2255779, 2255787, 2255794, 2255802, 2255810, 2255817, 2255825, 2255833, 2255841, 2255849, 2255856, 2255864, 2255872, 2255880, 2255888, 2255896, 2255904, 2255912, 2255920, 2255928, 2255936, 2255944, 2255952, 2255960, 2255968, 2255976, 2255984, 2255992, 2256001, 2256009, 2256017, 2256025, 2256033, 2256042, 2256050};
        compressor.encode(src, values);
        src.flip();
        int[] data = compressor.decode(src, values.length);
        System.out.println(Arrays.toString(data));
    }


}
