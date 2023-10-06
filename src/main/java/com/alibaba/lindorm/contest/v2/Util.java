package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.v1.Const;

public class Util {

    public static long parseTimestampKey(long timestamp){
        return timestamp - timestamp % (2 * 60 * 1000);
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

}
