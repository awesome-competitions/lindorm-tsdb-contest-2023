package com.alibaba.lindorm.contest.v1;

import com.alibaba.lindorm.contest.structs.Row;

import java.util.Arrays;
import java.util.function.BiConsumer;

public class Index {
    private final long[] positions;

    private long latestTimestamp;

    private long oldestTimestamp;

    private Row latestRow;

    private final int id;

    public Index(int id) {
        this.positions = new long[Const.INITIALIZE_VIN_POSITIONS_SIZE];
        for(int i = 0; i < Const.INITIALIZE_VIN_POSITIONS_SIZE; i++){
            this.positions[i] = -1;
        }
        this.id = id;
    }

    public int getId(){
        return id;
    }

    public void put(long timestamp, long position){
        put(timestamp, position, null);
    }

    public void put(long timestamp, long position, Row row){
        int index = this.getIndex(getSec(timestamp));
        if(timestamp > this.latestTimestamp){
            this.latestTimestamp = timestamp;
            this.latestRow = row;
        }
        if (this.oldestTimestamp == 0L || timestamp < this.oldestTimestamp){
            this.oldestTimestamp = timestamp;
        }
        this.positions[index] = position;
    }

    public long get(long timestamp) {
        return this.positions[getIndex(getSec(timestamp))];
    }

    public Row getLatestRow() {
        return latestRow;
    }

    public long getLatestTimestamp(){
        return this.latestTimestamp;
    }

    public long getOldestTimestamp() {
        return oldestTimestamp;
    }

    public long[] getPositions(){
        return positions;
    }

    public boolean isEmpty(){
        return this.latestTimestamp == 0L;
    }

    public void forRangeEach(long start, long end, BiConsumer<Long, Long> consumer){
        int startSec = this.getSec(Math.max(start, this.oldestTimestamp));
        int endSec = this.getSec(Math.min(end, this.latestTimestamp));
        for (int i = startSec; i <= endSec; i++) {
            int index = getIndex(i);
            if (this.positions[index] == -1){
                continue;
            }
            long t = i * 1000L;
            if (t < end && t >= start){
                consumer.accept(t,this.positions[index]);
            }
        }
    }

    private int getSec(long timestamp){
        return (int) (timestamp/1000);
    }

    private int getIndex(int sec){
        return sec % Const.INITIALIZE_VIN_POSITIONS_SIZE;
    }

    @Override
    public String toString() {
        return "Index{" +
                "positions=" + Arrays.toString(positions) +
                ", latestTimestamp=" + latestTimestamp +
                ", oldestTimestamp=" + oldestTimestamp +
                '}';
    }

    public static void main(String[] args) {
        System.out.println(1694078716012L/1000);
    }
}
