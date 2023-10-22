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
        int [] values = new int[]{1193917, 1193946, 1193975, 1194004, 1194033, 1194062, 1194091, 1194120, 1194149, 1194178, 1194207, 1194236, 1194265, 1194294, 1194323, 1194352, 1194381, 1194410, 1194439, 1194468, 1194498, 1194527, 1194556, 1194585, 1194614, 1194643, 1194673, 1194702, 1194731, 1194760, 1194789, 1194819, 1194848, 1194877, 1194906, 1194936, 1194965, 1194994, 1195024, 1195053, 1195082, 1195112, 1195141, 1195170, 1195200, 1195229, 1195258, 1195288, 1195317, 1195347, 1195376, 1195405, 1195435, 1195464, 1195494, 1195523, 1195553, 1195582, 1195612, 1195641, 1195671, 1195700, 1195730, 1195760, 1195789, 1195819, 1195848, 1195878, 1195907, 1195937, 1195967, 1195996, 1196026, 1196056, 1196085, 1196115, 1196145, 1196174, 1196204, 1196234, 1196264, 1196293, 1196323, 1196353, 1196383, 1196412, 1196442, 1196472, 1196502, 1196532, 1196561, 1196591, 1196621, 1196651, 1196681, 1196711, 1196741, 1196771, 1196800, 1196830, 1196860, 1196890, 1196920, 1196950, 1196980, 1197010, 1197040, 1197070, 1197100, 1197130, 1197160, 1197190, 1197220, 1197250, 1197280, 1197311, 1197341, 1197371, 1197401, 1197431, 1197461, 1197491, 1197521, 1197552, 1197582, 1197612, 1197642, 1197672, 1197703, 1197733, 1197763, 1197793, 1197824, 1197854, 1197884, 1197914, 1197945, 1197975, 1198005, 1198036, 1198066, 1198096, 1198127, 1198157, 1198187, 1198218, 1198248, 1198279, 1198309, 1198339, 1198370, 1198400, 1198431, 1198461, 1198492, 1198522, 1198553, 1198583, 1198614, 1198644, 1198675, 1198705, 1198736, 1198767, 1198797, 1198828, 1198858, 1198889, 1198920, 1198950, 1198981, 1199011, 1199042, 1199073, 1199103, 1199134, 1199165, 1199196, 1199226, 1199257, 1199288, 1199319, 1199349, 1199380, 1199411, 1199442, 1199472, 1199503, 1199534, 1199565, 1199596, 1199627, 1199657, 1199688, 1199719, 1199750, 1199781, 1199812, 1199843, 1199905};
        System.out.println(values.length);
        compressor.encode(src, values);
        src.flip();
        int[] data = compressor.decode(src, values.length);
        System.out.println(Arrays.toString(data));
    }


}
