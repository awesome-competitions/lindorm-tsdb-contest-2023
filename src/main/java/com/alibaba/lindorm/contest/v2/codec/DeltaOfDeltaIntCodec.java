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
        int [] values = new int[]{1922752, 1944225, 1922967, 1923075, 1923182, 1923290, 1923397, 1923505, 1923612, 1923719, 1923827, 1923934, 1924042, 1924149, 1924257, 1924365, 1924472, 1924580, 1924687, 1924795, 1924902, 1925010, 1925117, 1925225, 1925332, 1925440, 1925548, 1925655, 1925763, 1925870, 1925978, 1926085, 1926193, 1926301, 1926408, 1926516, 1926623, 1926731, 1926839, 1926946, 1927054, 1927162, 1927269, 1927377, 1927485, 1927592, 1927700, 1927807, 1927915, 1928023, 1928131, 1928238, 1928346, 1928454, 1928561, 1928669, 1928777, 1928884, 1928992, 1929100, 1929208, 1929315, 1929423, 1929531, 1929638, 1929746, 1929854, 1929962, 1930069, 1930177, 1930285, 1930393, 1930501, 1930608, 1930716, 1930824, 1930932, 1931039, 1931147, 1931255, 1931363, 1931471, 1931579, 1931686, 1931794, 1931902, 1932010, 1932118, 1932226, 1932333, 1932441, 1932549, 1932657, 1932765, 1932873, 1932981, 1933089, 1933196, 1933304, 1933412, 1933520, 1933628, 1933736, 1933844, 1933952, 1934060, 1934168, 1934276, 1934383, 1934491, 1934599, 1934707, 1934815, 1934923, 1935031, 1935139, 1935247, 1935355, 1935463, 1935571, 1935679, 1935787, 1935895, 1936003, 1936111, 1936219, 1936327, 1936435, 1936543, 1936651, 1936759, 1936867, 1936975, 1937083, 1937191, 1937300, 1937408, 1937516, 1937624, 1937732, 1937840, 1937948, 1938056, 1938164, 1938272, 1938380, 1938488, 1938597, 1938705, 1938813, 1938921, 1939029, 1939137, 1939245, 1939353, 1939462, 1939570, 1939678, 1939786, 1939894, 1940002, 1940111, 1940219, 1940327, 1940435, 1940543, 1940652, 1940760, 1940868, 1940976, 1941084, 1941193, 1941301, 1941409, 1941517, 1941626, 1941734, 1941842, 1941950, 1942059, 1942167, 1942275, 1942383, 1942492, 1942600, 1942708, 1942816, 1942925, 1943033, 1943141, 1943250, 1943358, 1943466, 1943575, 1943683, 1943791, 1943900, 1944008, 1944116, 1944225};
        System.out.println(values.length);
        compressor.encode(src, values);
        src.flip();
        int[] data = compressor.decode(src, values.length);
        System.out.println(Arrays.toString(data));
    }


}
