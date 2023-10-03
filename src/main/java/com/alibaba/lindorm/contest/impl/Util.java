package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.Vin;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class Util {

    private static Unsafe unsafe;

    private static Field address;

    static {
        try{
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            Field address = Buffer.class.getDeclaredField("address");

            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static int getInt(long address){
        int n = unsafe.getInt(address);
        return Integer.reverseBytes(n);
    }

    public static long getAddress(Buffer buffer)  {
        try {
            Field f = Buffer.class.getDeclaredField("address");
            f.setAccessible(true);
            return (long) f.get(buffer);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void copyMemory(long srcAddress, long destAddress, long bytes) {
        unsafe.copyMemory(srcAddress, destAddress, bytes);
    }

    public static void copyMemory(ByteBuffer src, int srcPos, ByteBuffer dst, int dstPos, long bytes) {
        long srcAddress = Util.getAddress(src);
        long dstAddress = Util.getAddress(dst);
        unsafe.copyMemory(srcAddress + srcPos, dstAddress + dstPos, bytes);
        src.position(srcPos + (int) bytes);
        dst.position(dstPos + (int) bytes);
    }

    public static long assembleLenAndPos(int len, long position){
        return ((long)len << 48) | position;
    }

    public static int getLen(long lenAndPos){
        return (int) (lenAndPos >>> 48);
    }

    public static long getPosition(long lenAndPos){
        return lenAndPos & 0x0000FFFFFFFFFFFFL;
    }

    public static void putNumber(ByteBuffer buffer, long v, int b){
        for (int i = 0; i < b; i ++){
            buffer.put((byte) (v >> (8*i)));
        }
    }

    public static long getNumber(ByteBuffer buffer, int b) {
        long result = 0;
        for (int i = 0; i < b; i++) {
            result |= ((long) (buffer.get() & 0xFF)) << (8 * i);
        }
        return result;
    }

    public static int calculateBits(long n, boolean carry) {
        int b = 0;
        while (n > 0){
            n >>= 1;
            b ++;
        }
        if(carry) {
            return b;
        }
        return b - 1;
    }

    public static int calculateBytes(long n, boolean carry) {
        int b = calculateBits(n, carry);
        return (b + 7) / 8;
    }

    public static int calculateDecimals(double n) {
        if (n % 1 == 0){
            return 0;
        }
        String s = ((Double)n).toString();
        return s.length() - s.indexOf(".") - 1;
    }

    public static int parseVinId(Vin vin){
        int vinId = 0;
        byte[] bs = vin.getVin();
        for (int i = Const.VIN_PREFIX.length(); i < bs.length; i ++){
            vinId *= 10;
            vinId += bs[i] - '0';
        }
        return vinId;
    }

    public static int expressTimestamp(long timestamp){
        return (int) ((timestamp - Const.BEGIN_TIMESTAMP)/1000);
    }

    public static long unExpressTimestamp(int timestamp){
        return timestamp * 1000L + Const.BEGIN_TIMESTAMP;
    }

    public static void main(String[] args) {
        System.out.println(calculateBits(-4, true));
    }
}
