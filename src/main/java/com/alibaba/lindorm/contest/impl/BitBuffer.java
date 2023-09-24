package com.alibaba.lindorm.contest.impl;

import java.nio.ByteBuffer;

public class BitBuffer {

    private final ByteBuffer buffer;

    private long pos;

    private long limit;

    private final long capacity;

    private byte b;

    private boolean mark;

    public BitBuffer(int capacity){
        this.buffer = ByteBuffer.allocateDirect(capacity);
        this.capacity = (long) capacity * Const.BITS;
        this.limit = this.capacity;
    }

    public BitBuffer(ByteBuffer buffer){
        this.buffer = buffer;
        this.capacity = (long) buffer.capacity() * Const.BITS;
        this.limit = this.capacity;
    }

    public void putInt(int v, int bits){
        putLong(v, bits);
    }

    public void putShort(short v){
        putShort(v, 16);
    }

    public void putShort(short v, int bits){
        putLong(v, bits);
    }

    public void putByte(byte v, int bits){
        putLong(v, bits);
    }

    public void putLong(long v, int bits){
        while (bits > 0){
            int bPos = (int) (pos % 8);
            int available = 8 - bPos;
            b |= v << bPos;
            int actual = Math.min(available, bits);
            bits -= actual;
            v >>= actual;
            if (available == actual){
                buffer.put(b);
                b = 0;
            }
            pos += actual;
        }
    }

    public long getLong(long pos, int bits){
        long v = 0;
        long startPos = pos;
        int surplusBits = bits;
        while (surplusBits > 0){
            byte b = buffer.get((int) (pos / 8));
            int bPos = (int) (pos % 8);
            int available = 8 - bPos;
            v |= (convertByte2Long(b) >> bPos) << (pos - startPos);
            int actual = Math.min(available, surplusBits);
            surplusBits -= actual;
            pos += actual;
        }
        return v & ((long)Math.pow(2, bits) - 1);
    }

    public long getLong(int bits){
        long v = 0;
        long startPos = pos;
        int surplusBits = bits;
        while (surplusBits > 0){
            if (!mark){
                b = buffer.get();
                mark = true;
            }
            int bPos = (int) (pos % 8);
            int available = 8 - bPos;
            v |= (convertByte2Long(b) >> bPos) << (pos - startPos);
            int actual = Math.min(available, surplusBits);
            surplusBits -= actual;
            if (available == actual && buffer.hasRemaining()){
                mark = false;
            }
            pos += actual;
        }
        return v & ((long)Math.pow(2, bits) - 1);
    }

    public void putBytes(byte[] bs){
        for (byte b : bs){
            putByte(b);
        }
    }

    public void putByte(byte b){
        putByte(b, 8);
    }

    public void put(BitBuffer buffer){
        for (int i = 0; i < buffer.limit(); i++) {
            putByte(buffer.getByte());
        }
    }

    public long getLong(){
        return getLong(64);
    }

    public int getInt(int bits){
        return (int) getLong(bits);
    }

    public int getInt(){
        return getInt(32);
    }

    public int getInt(long position, int bits){
        return (int) getLong(position, bits);
    }

    public short getShort(){
        return getShort(16);
    }

    public short getShort(int bits){
        return (short) getLong(bits);
    }

    public byte getByte(int bits){
        return (byte) getLong(bits);
    }

    public byte getByte(){
        return getByte(8);
    }

    public byte[] getBytes(int len){
        byte[] bs = new byte[len];
        for (int i = 0; i < len; i++){
            bs[i] = getByte();
        }
        return bs;
    }

    public int limit(){
        return buffer.limit();
    }

    public void limit(int limit){
        this.limit = (long) limit * Const.BITS;
        this.buffer.limit(limit);
    }

    public void position(int pos){
        this.pos = (long) pos * Const.BITS;
        this.mark = false;
        this.buffer.position(pos);
    }

    public int position(){
        return buffer.position();
    }

    public long getBitPosition(){
        return pos;
    }

    public void skip(int pos){
        this.pos += pos;
        this.mark = false;
        this.buffer.position((int) (this.pos / Const.BITS));
    }

    public void flip(){
        limit = pos;
        if (pos % 8 != 0){
            buffer.put(b);
        }
        pos = 0;
        buffer.flip();
        this.mark = false;
    }

    public void clear(){
        limit = capacity;
        pos = 0;
        b = 0;
        buffer.clear();
        this.mark = false;
    }

    public int capacity(){
        return buffer.capacity();
    }

    public int remaining(){
        return buffer.remaining();
    }

    public ByteBuffer buffer(){
        return buffer;
    }

    public void print(){
        for (int i = 0; i < 100; i ++){
            if (i < buffer.remaining()) {
                System.out.print(buffer.get(i) + " ");
            }
        }
        System.out.println();
    }

    public long convertByte2Long(byte b){
        if (b < 0){
            return b + 256;
        }
        return b;
    }

    public void printBinary(byte b){
        System.out.println(Integer.toBinaryString(b & 0xFF));
    }

    public void printBinary(long l){
        System.out.println(Long.toBinaryString(l));
    }

    public static void main(String[] args) {
        BitBuffer buf = new BitBuffer(165465);
//        buf.printBinary(buf.convertByte2Long((byte) -8));
//        buf.printBinary((byte)-8);
        buf.putLong(1689091211000L, Const.TIMESTAMP_BITS);
        buf.flip();
        buf.print();
        long l = buf.getLong(Const.TIMESTAMP_BITS);
        System.out.println(l);

        buf.printBinary(1689091211000L);
        buf.printBinary(l);

    }

}
