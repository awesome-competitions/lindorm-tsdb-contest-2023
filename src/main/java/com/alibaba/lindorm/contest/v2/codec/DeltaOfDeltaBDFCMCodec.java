package com.alibaba.lindorm.contest.v2.codec;

import com.alibaba.lindorm.contest.v2.Context;
import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaOfDeltaBDFCMCodec extends Codec<double[]>{
    @Override
    public void encode(ByteBuffer src, double[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putDouble(data[0]);
        if (data.length > 1){
            double preDiff = 0;
            double preDiffDiff = 0;
            int preLeadingZeros = -1;
            int preTrailingZeros = -1;
            for (int i = 1; i < size; i++) {
                double diff = data[i] - data[i - 1];
                double diffDiff = diff - preDiff;
                long v1 = Double.doubleToRawLongBits(diffDiff);
                long v2 = Double.doubleToRawLongBits(preDiffDiff);
                long xorValue = v1 ^ v2;
                int leadingZeros = Long.numberOfLeadingZeros(xorValue) / 4 * 4;
                int trailingZeros = Long.numberOfTrailingZeros(xorValue) / 4 * 4;
                if (xorValue == 0){
                    buffer.putBit(false);
                }else if (preTrailingZeros >= 0 && leadingZeros == preLeadingZeros && trailingZeros == preTrailingZeros) {
                    buffer.putBit(true);
                    buffer.putBit(true);
                    long v = xorValue >> preTrailingZeros;
                    buffer.putLong(v, 64 - preLeadingZeros - preTrailingZeros);
                }else {
                    buffer.putBit(true);
                    buffer.putBit(false);
                    buffer.putInt(leadingZeros/4, 4);
                    buffer.putInt(trailingZeros/4, 4);
                    long v = xorValue >> trailingZeros;
                    buffer.putLong(v, 64 - leadingZeros - trailingZeros);
                    preLeadingZeros = leadingZeros;
                    preTrailingZeros = trailingZeros;
                }
                preDiffDiff = diffDiff;
                preDiff = diff;
            }
        }
        buffer.flip();
    }

    @Override
    public void decode(ByteBuffer src, double[] data, int size) {
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = buffer.getDouble();
        if (size > 1){
            double preDiff = 0;
            double preDiffDiff = 0;
            int preLeadingZeros = -1;
            int preTrailingZeros = -1;
            for (int i = 1; i < size; i++) {
                long xorValue = 0;
                if (buffer.getBoolean()){
                    if (!buffer.getBoolean()) {
                        preLeadingZeros = buffer.getIntUnsigned(4) * 4;
                        preTrailingZeros = buffer.getIntUnsigned(4) * 4;
                    }
                    long v = buffer.getLongUnsigned(64 - preLeadingZeros - preTrailingZeros);
                    xorValue = v << preTrailingZeros;
                }
                long v1 = Double.doubleToRawLongBits(preDiffDiff);
                double diffDiff = Double.longBitsToDouble(v1 ^ xorValue);
                double diff = preDiff + diffDiff;
                data[i] = data[i-1] + diff;
                preDiffDiff = diffDiff;
                preDiff = diff;
            }
        }
    }

    public static void main(String[] args) {
        DeltaOfDeltaBDFCMCodec varintCodec = new DeltaOfDeltaBDFCMCodec();
//        double[] numbers = {-17631.700458325424,-17631.700771279953,-17631.701292870832,-17631.70202309806,-17631.702961961626,-17631.704109461523,-17631.705465597748,-17631.70703037029,-17631.70880377913,-17631.71078582426,-17631.712976505667,-17631.715375823325,-17631.717983777227,-17631.720800367348,-17631.723825593668};
        double[] numbers = {20155.051924321,20155.051990041367,20155.05205784809,20155.052127741168,20155.0521997206,20155.05227378639,20155.05234993853,20155.052428177023,20155.052508501867,20155.052590913063,20155.05267541061,20155.05276199451,20155.052850664757,20155.052941421352,20155.053034264296,20155.053129193588,20155.053226209224,20155.053325311208,20155.05342649954,20155.053529774214,20155.05363513523,20155.05374258259,20155.053852116293,20155.053963736336,20155.05407744272,20155.054193235443,20155.054311114505,20155.054431079905,20155.054553131642,20155.054677269716,20155.054803494124,20155.054931804865,20155.055062201936,20155.055194685345,20155.055329255083,20155.055465911148,20155.055604653546,20155.05574548227,20155.05588839732,20155.056033398698,20155.056180486397,20155.056329660423,20155.05648092077,20155.05663426744,20155.056789700426,20155.056947219735,20155.05710682536,20155.0572685173,20155.057432295558,20155.05759816013,20155.05776611101,20155.057936148205,20155.05810827171,20155.058282481525,20155.058458777643,20155.058637160073,20155.058817628804,20155.05900018384,20155.059184825175,20155.059371552812,20155.05956036675,20155.059751266985,20155.059944253517,20155.060139326342,20155.06033648546,20155.060535730874,20155.060737062573,20155.060940480565,20155.06114598484};
//        double[] numbers = {9973.39245805256,9973.397330989092,9973.383986099192,9973.38891858201,9973.371878204334,9973.376779902414,9973.359826356242,9973.357454257546,9973.362384929032,9973.286227238877,9973.291100168366,9973.274285352003,9973.27918715265,9973.26587114058,9973.270772941281};
//        double[] numbers = {250267.57624034025,250267.57936988556,250267.58458579436,250267.5918880666,250267.6012767023,250267.61275170126,250267.6263130635,250267.64196078893,250267.65969487734,250267.67951532867,250267.70142214268,250267.72541531932,250267.7514948583,250267.7796607595,250267.80991302268};
//        double[] numbers = {-2226421.1215246054,-2226421.152820058,-2226421.2049791464,-2226421.278001869,-2226421.3718882254,-2226421.4866382154,-2226421.622251838,-2226421.778729092,-2226421.956069976,-2226422.1542744893,-2226422.3733426295,-2226422.6132743955,-2226422.8740697857,-2226423.155728798,-2226423.4582514297};
//        double[] numbers = {22614286.67410786,22614286.98706239,22614287.50865327,22614288.238880496,22614289.177744064,22614290.32524396,22614291.681380186,22614293.246152725,22614295.019561566,22614297.0016067,22614299.192288104,22614301.591605764,22614304.199559666,22614307.016149785,22614310.041376106};

//        double[] numbers = {-13061,-14901,-22085,-13557,-15621,-18085,-16757,-19525,-17285,-15253,-13013,-17045,-20613,-17941,-13285,-19381};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
        varintCodec.encode(encodedBuffer, numbers, numbers.length);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());

        int size = numbers.length;
        System.out.println(size * 8);
        System.out.println("==============================");
        varintCodec.decode(encodedBuffer, Context.getBlockDoubleValues(), size);

        System.out.println(Arrays.toString(Context.getBlockDoubleValues()));
    }
}
