package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Formattable;

public class HuffmanDeltaOfDeltaIntCodec extends Codec<int[]> {

    public int calculateLevel(int[] data, int size){
        int preDiff = data[1] - data[0];
        int maxDiff = Integer.MIN_VALUE;
        for (int i = 2; i < size; i++) {
            int diff = data[i] - data[i - 1];
            int v = encodeZigzag(diff - preDiff);
            if (v > maxDiff){
                maxDiff = v;
            }
            preDiff = diff;
        }
        int level = 3;
        if (maxDiff < 5){
            level = 2;
        }
        if (maxDiff < 3){
            level = 1;
        }
        return level;
    }


    @Override
    public void encode(ByteBuffer src, int[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        encodeVarInt(buffer, data[0]);
        if (data.length > 1){
            int preDiff = data[1] - data[0];
            encodeVarInt(buffer, preDiff);
            int level = calculateLevel(data, size);
            System.out.println("level " + level);
            buffer.putInt(level, 2);
            for (int i = 2; i < size; i++) {
                int diff = data[i] - data[i - 1];
                int v = encodeZigzag(diff - preDiff);
                switch (level){
                    case 1:
                        if (v == 0){
                            buffer.putBit(false);
                        } else if (v == 1) {
                            buffer.putBit(true);
                            buffer.putBit(true);
                        } else if (v == 2) {
                            buffer.putBit(true);
                            buffer.putBit(false);
                        } else{
                            throw new RuntimeException("invalid value " + v + " in level 1");
                        }
                        break;
                    case 2:
                        if (v == 0){
                            buffer.putBit(false);
                        } else if (v == 1) {
                            buffer.putBit(true);
                            buffer.putBit(true);
                        } else if (v == 2) {
                            buffer.putBit(true);
                            buffer.putBit(false);
                            buffer.putBit(true);
                        } else if (v == 3) {
                            buffer.putBit(true);
                            buffer.putBit(false);
                            buffer.putBit(false);
                            buffer.putBit(true);
                        } else if (v == 4) {
                            buffer.putBit(true);
                            buffer.putBit(false);
                            buffer.putBit(false);
                            buffer.putBit(false);
                        } else{
                            throw new RuntimeException("invalid value " + v + " in level 2");
                        }
                        break;
                    case 3:
                        if (v == 0){
                            buffer.putBit(true);
                            buffer.putBit(false);
                        } else if (v == 1) {
                            buffer.putBit(false);
                            buffer.putBit(true);
                        } else if (v == 2) {
                            buffer.putBit(false);
                            buffer.putBit(false);
                        } else if (v == 3) {
                            buffer.putBit(true);
                            buffer.putBit(true);
                            buffer.putBit(false);
                        } else if (v == 4) {
                            buffer.putBit(true);
                            buffer.putBit(true);
                            buffer.putBit(true);
                            buffer.putBit(true);
                        } else if (v == 5) {
                            buffer.putBit(true);
                            buffer.putBit(true);
                            buffer.putBit(true);
                            buffer.putBit(false);
                            buffer.putBit(true);
                        } else if (v == 6) {
                            buffer.putBit(true);
                            buffer.putBit(true);
                            buffer.putBit(true);
                            buffer.putBit(false);
                            buffer.putBit(false);
                        }else{
                            throw new RuntimeException("invalid value " + v + " in level 2");
                        }
                        break;
                    default:
                        throw new RuntimeException("invalid level " + level);
                }
                preDiff = diff;
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
            int level = buffer.getIntUnsigned(2);
            int preDiff = data[1] - data[0];
            for (int i = 2; i < size; i++) {
                int v = 0;
                switch (level){
                    case 1:
                        if (buffer.getBoolean()){
                            v = buffer.getBoolean() ? 1 : 2;
                        }
                        break;
                    case 2:
                        if (buffer.getBoolean()){
                            if (buffer.getBoolean()){
                                v = 1;
                            }else if (buffer.getBoolean()){
                                v = 2;
                            }else if (buffer.getBoolean()){
                                v = 3;
                            }else{
                                v = 4;
                            }
                        }
                        break;
                    case 3:
                        if (buffer.getBoolean()) {
                            if (buffer.getBoolean()) {
                                if (buffer.getBoolean()){
                                    if (buffer.getBoolean()){
                                        v = 4;
                                    }else{
                                        if (buffer.getBoolean()){
                                            v = 5;
                                        }else{
                                            v = 6;
                                        }
                                    }
                                }else{
                                    v = 3;
                                }
                            }
                        }else{
                            if (buffer.getBoolean()){
                                v = 1;
                            }else{
                                v = 2;
                            }
                        }
                        break;
                }
                int diff = decodeZigzag(v) + preDiff;
                data[i] = data[i - 1] + diff;
                preDiff = diff;
            }
        }
    }

    public static void main(String[] args) {
        HuffmanDeltaOfDeltaIntCodec compressor = new HuffmanDeltaOfDeltaIntCodec();


        int[][] valuesList = new int[][]{
            {-18056715, -18056715, -18056715, -18056716, -18056716, -18056717, -18056718, -18056719, -18056720, -18056722, -18056723, -18056725, -18056727, -18056729, -18056731, -18056734, -18056736, -18056739, -18056742, -18056745, -18056748, -18056752, -18056755, -18056759, -18056763, -18056767, -18056771, -18056776, -18056780, -18056785, -18056790, -18056795, -18056800, -18056806, -18056811, -18056817, -18056823, -18056829, -18056835, -18056842, -18056848, -18056855, -18056862, -18056869, -18056876, -18056884, -18056891, -18056899, -18056907, -18056915, -18056923, -18056932, -18056941, -18056949, -18056958, -18056967, -18056977, -18056986, -18056996, -18057005, -18057015, -18057025, -18057036, -18057046, -18057057, -18057067, -18057078, -18057089, -18057101, -18057112, -18057124, -18057136, -18057148, -18057160, -18057172, -18057184, -18057197, -18057210, -18057223, -18057236, -18057249, -18057262, -18057276, -18057290, -18057304, -18057318, -18057332, -18057347, -18057361, -18057376, -18057391, -18057406, -18057421, -18057437, -18057452, -18057468, -18057484, -18057500, -18057516, -18057533, -18057549, -18057566, -18057583, -18057600, -18057618, -18057635, -18057653, -18057670, -18057688, -18057706, -18057725, -18057743, -18057762, -18057781, -18057799, -18057819, -18057838, -18057857, -18057877, -18057897, -18057917, -18057937, -18057957, -18057977, -18057998, -18058019, -18058040, -18058061, -18058082, -18058104, -18058125, -18058147, -18058169, -18058191, -18058213, -18058236, -18058258, -18058281, -18058304, -18058327, -18058351, -18058374, -18058398, -18058421, -18058445, -18058470, -18058494, -18058518, -18058543, -18058568, -18058593, -18058618, -18058643, -18058669, -18058694, -18058720, -18058746, -18058772, -18058798, -18058825, -18058851, -18058878, -18058905, -18058932, -18058960, -18058987, -18059015, -18059042, -18059070, -18059098, -18059127, -18059155, -18059184, -18059213, -18059242, -18059271, -18059300, -18059329, -18059359, -18059389, -18059419, -18059449, -18059479, -18059510, -18059540, -18059571, -18059602, -18059633, -18059665, -18059696, -18059728, -18059759, -18059791, -18059824, -18059856, -18059888, -18059921, -18059954, -18059987, -18060020},
            {2254311, 2254311, 2254311, 2254311, 2254311, 2254311, 2254312, 2254312, 2254312, 2254312, 2254312, 2254312, 2254313, 2254313, 2254313, 2254314, 2254314, 2254314, 2254315, 2254315, 2254315, 2254316, 2254316, 2254317, 2254317, 2254318, 2254318, 2254319, 2254319, 2254320, 2254321, 2254321, 2254322, 2254323, 2254323, 2254324, 2254325, 2254325, 2254326, 2254327, 2254328, 2254329, 2254330, 2254330, 2254331, 2254332, 2254333, 2254334, 2254335, 2254336, 2254337, 2254338, 2254339, 2254340, 2254342, 2254343, 2254344, 2254345, 2254346, 2254347, 2254349, 2254350, 2254351, 2254353, 2254354, 2254355, 2254357, 2254358, 2254359, 2254361, 2254362, 2254364, 2254365, 2254367, 2254368, 2254370, 2254371, 2254373, 2254375, 2254376, 2254378, 2254380, 2254381, 2254383, 2254385, 2254387, 2254388, 2254390, 2254392, 2254394, 2254396, 2254398, 2254399, 2254401, 2254403, 2254405, 2254407, 2254409, 2254411, 2254413, 2254415, 2254418, 2254420, 2254422, 2254424, 2254426, 2254428, 2254431, 2254433, 2254435, 2254437, 2254440, 2254442, 2254444, 2254447, 2254449, 2254452, 2254454, 2254456, 2254459, 2254461, 2254464, 2254466, 2254469, 2254472, 2254474, 2254477, 2254479, 2254482, 2254485, 2254487, 2254490, 2254493, 2254496, 2254498, 2254501, 2254504, 2254507, 2254510, 2254513, 2254516, 2254519, 2254522, 2254525, 2254527, 2254531, 2254534, 2254537, 2254540, 2254543, 2254546, 2254549, 2254552, 2254555, 2254559, 2254562, 2254565, 2254568, 2254572, 2254575, 2254578, 2254582, 2254585, 2254588, 2254592, 2254595, 2254599, 2254602, 2254606, 2254609, 2254613, 2254616, 2254620, 2254623, 2254627, 2254631, 2254634, 2254638, 2254642, 2254645, 2254649, 2254653, 2254657, 2254661, 2254664, 2254668, 2254672, 2254676, 2254680, 2254684, 2254688, 2254692, 2254696, 2254700, 2254704, 2254708, 2254712, 2254716, 2254720, 2254724},
            {1153530, 1153530, 1153530, 1153530, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153531, 1153532, 1153532, 1153532, 1153532, 1153532, 1153533, 1153533, 1153533, 1153533, 1153533, 1153534, 1153534, 1153534, 1153535, 1153535, 1153535, 1153535, 1153536, 1153536, 1153536, 1153537, 1153537, 1153538, 1153538, 1153538, 1153539, 1153539, 1153540, 1153540, 1153541, 1153541, 1153541, 1153542, 1153542, 1153543, 1153543, 1153544, 1153545, 1153545, 1153546, 1153546, 1153547, 1153547, 1153548, 1153549, 1153549, 1153550, 1153550, 1153551, 1153552, 1153552, 1153553, 1153554, 1153555, 1153555, 1153556, 1153557, 1153557, 1153558, 1153559, 1153560, 1153561, 1153561, 1153562, 1153563, 1153564, 1153565, 1153565, 1153566, 1153567, 1153568, 1153569, 1153570, 1153571, 1153572, 1153573, 1153574, 1153575, 1153576, 1153577, 1153578, 1153578, 1153580, 1153581, 1153582, 1153583, 1153584, 1153585, 1153586, 1153587, 1153588, 1153589, 1153590, 1153591, 1153592, 1153594, 1153595, 1153596, 1153597, 1153598, 1153599, 1153601, 1153602, 1153603, 1153604, 1153606, 1153607, 1153608, 1153609, 1153611, 1153612, 1153613, 1153615, 1153616, 1153617, 1153619, 1153620, 1153621, 1153623, 1153624, 1153625, 1153627, 1153628, 1153630, 1153631, 1153633, 1153634, 1153636, 1153637, 1153639, 1153640, 1153642, 1153643, 1153645, 1153646, 1153648, 1153649, 1153651, 1153653, 1153654, 1153656, 1153657, 1153659, 1153661, 1153662, 1153664, 1153666, 1153667, 1153669, 1153671, 1153672, 1153674, 1153676, 1153678, 1153679, 1153681, 1153683, 1153685, 1153687, 1153688, 1153690, 1153692, 1153694, 1153696, 1153698, 1153699, 1153701, 1153703, 1153705, 1153707, 1153709, 1153711, 1153713, 1153715, 1153717, 1153719, 1153721, 1153723, 1153725, 1153727, 1153729, 1153731, 1153733, 1153735, 1153737},
            {1,1,1,2,2,2,4,4,4,7,7,7,10,10,10},
        };
        for (int[] values: valuesList){
            ByteBuffer src = ByteBuffer.allocate(1000);
            compressor.encode(src, values, values.length);
            src.flip();

            System.out.println(src.limit());
            System.out.println(values.length*4);
            compressor.decode(src, Context.getBlockIntValues(), values.length);
            for (int i = 0; i < values.length; i++) {
                if (values[i] != Context.getBlockIntValues()[i])
                    throw new RuntimeException("error " + values[i] + " " + Context.getBlockIntValues()[i] + " " + i);
            }
        }
    }
}
