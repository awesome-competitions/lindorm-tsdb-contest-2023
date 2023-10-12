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

    public static long assemblePosLen(int len, long position){
        return ((long)len << 40) | position;
    }

    public static int parseLen(long lenAndPos){
        return (int) (lenAndPos >>> 40);
    }

    public static long parsePos(long lenAndPos){
        return lenAndPos & 0x000000FFFFFFFFFFL;
    }

    public static void main(String[] args) {
        long pos = 100 * 1024 * 1024 * 1024L;
        int size = 8388608;
        long lenAndPos = assemblePosLen(size, pos);
        System.out.println(parseLen(lenAndPos));
        System.out.println(parsePos(lenAndPos));

    }

}
