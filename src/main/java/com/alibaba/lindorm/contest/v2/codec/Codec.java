package com.alibaba.lindorm.contest.v2.codec;

import net.magik6k.bitbuffer.ArrayBitBuffer;
import net.magik6k.bitbuffer.BitBuffer;

import java.nio.ByteBuffer;

public abstract class Codec<T> {

    public abstract void encode(ByteBuffer src, T data, int size);

    public abstract void decode(ByteBuffer src, T data, int size);

    public static Codec<int[]> deltaIntCodec(int deltaSize){
        return new DeltaIntCodec(deltaSize);
    }

    public static Codec<int[]> huffmanCodec(int min, int max){
        int size = max - min + 1;
        switch (size){
            case 3:
                return new Huffman3Codec(min);
            case 5:
                return new Huffman5Codec(min);
            case 9:
                return new Huffman9Codec(min);
        }
        throw new RuntimeException("invalid size " + size);
    }

    public static Codec<ByteBuffer[]> stringHuffman3Codec(int min){
        return new StringHuffman3Codec(min);
    }

    public static Codec<double[]> xorDoubleCodec(){
        return new XORDoubleCodec();
    }

    public static Codec<double[]> dfcmCodec(){
        return new DFCMCodec();
    }

    public static Codec<double[]> bdfcmCodec(){
        return new BDFCMCodec();
    }

    public static Codec<double[]> simpleBDFCMCodec(){
        return new SimpleBDFCMCodec();
    }

    public static Codec<double[]> deltaOfDeltaDFCMCodec(){
        return new DeltaOfDeltaDFCMCodec();
    }

    public static Codec<double[]> deltaOfDeltaBDFCMCodec(){
        return new DeltaOfDeltaBDFCMCodec();
    }

    public static Codec<double[]> deltaOfDeltaBFCMPlusCodec(){
        return new DeltaOfDeltaBFCMPlusCodec();
    }

    public static Codec<double[]> zstdDoubleCodec(){
        return new ZSTDDoubleCodec();
    }

    public static Codec<ByteBuffer[]> stringCodec(){
        return new StringCodec(0);
    }

    public static Codec<ByteBuffer[]> originalStringCodec(){
        return new OriginalStringCodec(0);
    }

    public static Codec<ByteBuffer[]> stringCodec(int fixedSize){
        return new StringCodec(fixedSize);
    }

    public static Codec<ByteBuffer[]> originalStringCodec(int fixedSize){
        return new OriginalStringCodec(fixedSize);
    }

    public static Codec<int[]> deltaOfDeltaIntCodec(){
        return new DeltaOfDeltaIntCodec();
    }

    public static Codec<int[]> huffmanDeltaOfDeltaIntCodec(){
        return new HuffmanDeltaOfDeltaIntCodec();
    }

    public static Codec<int[]> runLengthIntCodec(int runLengthMaxSize){
        return new RunLengthIntCodec(runLengthMaxSize);
    }

    public static Codec<int[]> fixedIntCodec(int min, int max){
        return new FixedIntCodec(min, max);
    }

    public static Codec<int[]> varIntCodec(){
        return new VarIntCodec();
    }

    public static Codec<int[]> deltaVarIntCodec(){
        return new DeltaVarIntCodec();
    }

    public static Codec<int[]> dfcmIntCodec(){
        return new DFCMIntCodec();
    }

    public static Codec<int[]> simpleNBCodec(){
        return new SimpleNBCodec();
    }

    public static Codec<int[]> bdfcmIntCodec(){
        return new BDFCMIntCodec();
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
                return result;
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
