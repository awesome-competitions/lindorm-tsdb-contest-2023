package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.util.Map;

public class Index {

    private final int vin;

    private final long[] positions;

    private long latestTimestamp;

    private long oldestTimestamp;

    private final Data data;

    private Block block;

    public Index(Data data, int vin){
        this.vin = vin;
        this.data = data;
        this.positions = new long[Const.INDEX_POSITIONS_SIZE];
    }

    public void insert(long timestamp, Map<String, ColumnValue> columns){
        if(timestamp > latestTimestamp){
            latestTimestamp = timestamp;
        }
        if (oldestTimestamp == 0L || timestamp < oldestTimestamp){
            oldestTimestamp = timestamp;
        }

        long key = Util.parseTimestampKey(timestamp);
        if (block == null){
            block = new Block(data);
        }


        int index = getIndex(timestamp);
    }


    public Block getBlock() {
        return block;
    }

    public void setBlock(Block block) {
        this.block = block;
    }

    private static int getSec(long timestamp){
        return (int) (timestamp/1000);
    }

    private static int getIndex(long timestamp){
        return getSec(timestamp) % Const.TIME_SPAN;
    }
}
