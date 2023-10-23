package com.alibaba.lindorm.contest.v2.codec;

import net.magik6k.bitbuffer.BitBuffer;
import net.magik6k.bitbuffer.DirectBitBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DeltaOfDeltaXORDoubleCodec extends Codec<double[]>{
    @Override
    public void encode(ByteBuffer src, double[] data) {
        BitBuffer buffer = new DirectBitBuffer(src);
        buffer.putDouble(data[0]);
        if (data.length > 1){
            buffer.putDouble(data[1]);
            double preDiff = data[1] - data[0];
            double preDiffDiff = preDiff;
            int preLeadingZeros = -1;
            int preTrailingZeros = -1;
            for (int i = 2; i < data.length; i++) {
                double diff = data[i] - data[i - 1];
                double diffDiff = diff - preDiff;
                long v1 = Double.doubleToRawLongBits(diffDiff);
                long v2 = Double.doubleToRawLongBits(preDiffDiff);
                long xorValue = v1 ^ v2;
                int leadingZeros = Long.numberOfLeadingZeros(xorValue);
                int trailingZeros = Long.numberOfTrailingZeros(xorValue);
                if (xorValue == 0){
                    buffer.putBit(false);
                }else if (preLeadingZeros >= 0 && leadingZeros >= preLeadingZeros && trailingZeros >= preTrailingZeros) {
                    buffer.putBit(true);
                    buffer.putBit(true);
                    encodeVarLong(buffer, xorValue >> preTrailingZeros);
                }else {
                    buffer.putBit(true);
                    buffer.putBit(false);
                    buffer.putInt(trailingZeros, 6);
                    encodeVarLong(buffer, xorValue >> trailingZeros);
                }
                preLeadingZeros = leadingZeros;
                preTrailingZeros = trailingZeros;
                preDiffDiff = diffDiff;
                preDiff = diff;
            }
        }
        buffer.flip();
    }

    @Override
    public double[] decode(ByteBuffer src, int size) {
        double[] data = new double[size];
        BitBuffer buffer = new DirectBitBuffer(src);
        data[0] = buffer.getDouble();
        if (size > 1){
            data[1] = buffer.getDouble();
            double preDiff = data[1] - data[0];
            double preDiffDiff = preDiff;
            int preTrailingZeros = -1;
            for (int i = 2; i < size; i++) {
                long xorValue = 0;
                if (buffer.getBoolean()){
                    if (buffer.getBoolean()){
                        xorValue = decodeVarLong(buffer) << preTrailingZeros;
                    }else{
                        preTrailingZeros = buffer.getIntUnsigned(6);
                        xorValue = decodeVarLong(buffer) << preTrailingZeros;
                    }
                }
                long v1 = Double.doubleToRawLongBits(preDiffDiff);
                double diffDiff = Double.longBitsToDouble(v1 ^ xorValue);
                double diff = preDiff + diffDiff;
                data[i] = data[i-1] + diff;
                preDiffDiff = diffDiff;
                preDiff = diff;
            }
        }
        return data;
    }

    public static void main(String[] args) {
        DeltaOfDeltaXORDoubleCodec varintCodec = new DeltaOfDeltaXORDoubleCodec();
        double[] numbers = {-17631.700458325424,-17631.700771279953,-17631.701292870832,-17631.70202309806,-17631.702961961626,-17631.704109461523,-17631.705465597748,-17631.70703037029,-17631.70880377913,-17631.71078582426,-17631.712976505667,-17631.715375823325,-17631.717983777227,-17631.720800367348,-17631.723825593668};
//        double[] numbers = {20155.050922867114,20155.050925996657,20155.05093121257,20155.05093851484,20155.050947903474,20155.050959378474,20155.050972939836,20155.05098858756,20155.05100632165,20155.0510261421,20155.051048048914,20155.051072042093,20155.051098121632,20155.051126287533,20155.051156539794};

        ByteBuffer encodedBuffer = ByteBuffer.allocate(3000);
        varintCodec.encode(encodedBuffer, numbers);

        encodedBuffer.flip();
        System.out.println(encodedBuffer.remaining());

        int size = numbers.length;
        System.out.println(size * 8);
        System.out.println("==============================");
        double[] decodedNumbers = varintCodec.decode(encodedBuffer, size);

        System.out.println(Arrays.toString(decodedNumbers));
    }
}
