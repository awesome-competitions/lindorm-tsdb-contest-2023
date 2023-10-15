package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.v1.Const;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;

public class Util {

    private static Unsafe unsafe;

    static {
        try{
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        }catch (Exception e){
            e.printStackTrace();
        }
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

    public static int parseVinId(Vin vin){
        int vinId = 0;
        byte[] bs = vin.getVin();
        for (int i = Const.VIN_PREFIX.length(); i < bs.length; i ++){
            vinId *= 10;
            vinId += bs[i] - '0';
        }
        return vinId;
    }

    public static long assemblePosIndex(int index, long position){
        boolean negative = position < 0;
        if (negative){
            position = - position;
        }
        position |= ((long) index << 40);
        if (negative){
            position = - position;
        }
        return position;
    }

    public static long parsePos(long position){
        boolean negative = position < 0;
        if (negative){
            position = - position;
        }
        position &= 0x000000FFFFFFFFFFL;
        if (negative){
            position = - position;
        }
        return position;
    }

    public static int parseIndex(long position){
        if (position < 0){
            position = - position;
        }
        return (int) (position >>> 40);
    }

    public static int parseBits(long n, boolean unsigned) {
        int b = unsigned ? 0 : 1;
        while (n > 0){
            n >>= 1;
            b ++;
        }
        return b;
    }

    public static void main(String[] args) {
        System.out.println(parseBits(3, false));
    }

}
