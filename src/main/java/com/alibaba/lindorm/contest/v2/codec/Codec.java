package com.alibaba.lindorm.contest.v2.codec;

import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;

import java.nio.ByteBuffer;

public abstract class Codec<T> {

    public abstract void encode(ByteBuffer src, T data);

    public abstract T decode(ByteBuffer src, int size);

    public static Codec<int[]> deltaIntCodec(int deltaSize){
        return new DeltaIntCodec(deltaSize);
    }

    public static Codec<double[]> xorDoubleCodec(){
        return new XORDoubleCodec();
    }

    public static Codec<ByteBuffer> bytesCodec(){
        return new BytesCodec();
    }

    public static Codec<int[]> deltaOfDeltaIntCodec(int deltaSize){
        return new DeltaOfDeltaIntCodec(deltaSize);
    }

    public static Codec<long[]> deltaLongCodec(int deltaSize){
        return new DeltaLongCodec(deltaSize);
    }

    public static Codec<int[]> runLengthIntCodec(int runLengthMaxSize){
        return new RunLengthIntCodec(runLengthMaxSize);
    }

    public static Codec<int[]> varIntCodec(){
        return new VarIntCodec();
    }

    public static Codec<int[]> deltaVarIntCodec(){
        return new DeltaVarIntCodec();
    }

    // zigzag encode
    protected static int encodeZigzag(int n){
         return (n << 1) ^ n >> 31;
    }

    protected static int decodeZigzag(int m){
        return (m >>> 1) ^ -(m & 1);
    }

    // varint encode
    protected void encodeVarInt(BitBuffer dst, int value) {
        value = encodeZigzag(value);
        while ((value & 0xFFFFFF80) != 0) {
            dst.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        dst.put((byte) (value & 0x7F));
    }

    protected int decodeVarInt(BitBuffer src) {
        int result = 0;
        int shift = 0;
        while (true) {
            byte b = src.getByte();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return decodeZigzag(result);
            }
            shift += 7;
        }
    }

    protected static long encodeZigzag(long n) {
        return (n << 1) ^ (n >> 63);
    }

    protected static long decodeZigzag(long m) {
        return (m >>> 1) ^ -(m & 1);
    }

    // varlong encode
    protected static void encodeVarLong(BitBuffer dst, long value) {
        value = encodeZigzag(value);
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0) {
            dst.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        dst.put((byte) (value & 0x7F));
    }

    protected static long decodeVarLong(BitBuffer src) {
        long result = 0;
        int shift = 0;
        while (true) {
            byte b = src.getByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return decodeZigzag(result);
            }
            shift += 7;
        }
    }

    public static void main(String[] args) {
        long[] data = new long[]{123456456456L, 456464646546L, -4545646546546546L};

        BitBuffer buffer = new ArrayBitBuffer(10000);
        for (long datum : data) {
            encodeVarLong(buffer, datum);
        }
        buffer.flip();
        for (int i = 0; i < data.length; i++) {
            System.out.println(decodeVarLong(buffer));
        }
    }

}
