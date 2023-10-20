package com.alibaba.lindorm.contest.v2.codec;

import java.nio.ByteBuffer;

public abstract class Codec<T> {

    public abstract void encode(ByteBuffer src, T data);

    public abstract T decode(ByteBuffer src, int size);

    public static Codec<int[]> deltaIntCodec(int deltaSize){
        return new DeltaIntCodec(deltaSize);
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

    // zigzag encode
    protected static int encodeZigzag(int n){
         return (n << 1) ^ n >> 31;
    }

    protected static int decodeZigzag(int m){
        return (m >>> 1) ^ -(m & 1);
    }

    // varint encode
    protected void encodeVarInt(ByteBuffer dst, int value) {
        while ((value & 0xFFFFFF80) != 0) {
            dst.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        dst.put((byte) (value & 0x7F));
    }

    protected int decodeVarInt(ByteBuffer src) {
        int result = 0;
        int shift = 0;
        while (true) {
            byte b = src.get();
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
        }
    }

}
