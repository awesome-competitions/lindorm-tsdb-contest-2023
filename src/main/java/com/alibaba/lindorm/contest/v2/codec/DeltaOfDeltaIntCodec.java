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
        int preDiff = 0;
        for (int i = 1; i < data.length; i++) {
            int diff = data[i] - data[i - 1];
            if (Math.abs(diff - preDiff) > deltaSize) {
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
            data[i] = data[i - 1] + diff;
            preDiff = diff;
        }
        return data;
    }

    public static void main(String[] args) {
        Codec<int[]> compressor = new DeltaOfDeltaIntCodec(15);
        ByteBuffer src = ByteBuffer.allocate(1000);
        int [] values = new int[]{2254415, 2254415, 2254415, 2254415, 2254415, 2254415, 2254415, 2254415, 2254416, 2254416, 2254416, 2254416, 2254416, 2254417, 2254417, 2254417, 2254418, 2254418, 2254418, 2254419, 2254419, 2254420, 2254420, 2254421, 2254421, 2254422, 2254422, 2254423, 2254423, 2254424, 2254424, 2254425, 2254426, 2254426, 2254427, 2254428, 2254429, 2254429, 2254430, 2254431, 2254432, 2254433, 2254433, 2254434, 2254435, 2254436, 2254437, 2254438, 2254439, 2254440, 2254441, 2254442, 2254443, 2254444, 2254445, 2254447, 2254448, 2254449, 2254450, 2254451, 2254453, 2254454, 2254455, 2254456, 2254458, 2254459, 2254460, 2254462, 2254463, 2254465, 2254466, 2254468, 2254469, 2254471, 2254472, 2254474, 2254475, 2254477, 2254478, 2254480, 2254482, 2254483, 2254485, 2254487, 2254489, 2254490, 2254492, 2254494, 2254496, 2254498, 2254499, 2254501, 2254503, 2254505, 2254507, 2254509, 2254511, 2254513, 2254515, 2254517, 2254519, 2254521, 2254524, 2254526, 2254528, 2254530, 2254532, 2254534, 2254537, 2254539, 2254541, 2254544, 2254546, 2254548, 2254551, 2254553, 2254555, 2254558, 2254560, 2254563, 2254565, 2254568, 2254570, 2254573, 2254575, 2254578, 2254581, 2254583, 2254586, 2254589, 2254591, 2254594, 2254597, 2254600, 2254602, 2254605, 2254608, 2254611, 2254614, 2254617, 2254619, 2254622, 2254625, 2254628, 2254631, 2254634, 2254637, 2254640, 2254643, 2254647, 2254650, 2254653, 2254656, 2254659, 2254662, 2254666, 2254669, 2254672, 2254675, 2254679, 2254682, 2254685, 2254689, 2254692, 2254696, 2254699, 2254702, 2254706, 2254709, 2254713, 2254716, 2254720, 2254724, 2254727, 2254731, 2254734, 2254738, 2254742, 2254746, 2254749, 2254753, 2254757, 2254761, 2254764, 2254768, 2254772, 2254776, 2254780, 2254784, 2254788, 2254792, 2254796, 2254804, 2254800, 2254808, 2254812, 2254816, 2254820, 2254824, 2254828};
        compressor.encode(src, values);
        src.flip();
        int[] data = compressor.decode(src, values.length);
        System.out.println(Arrays.toString(data));
    }


}
