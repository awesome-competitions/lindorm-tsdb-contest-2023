package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.Row;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public class Index {
    private final long[] positions;

    private long latestTimestamp;

    private long oldestTimestamp;

    private Row lastestRow;

    private final ReentrantLock lock;

    public Index() {
        this.positions = new long[Const.INITIALIZE_VIN_POSITIONS_SIZE];
        for(int i = 0; i < Const.INITIALIZE_VIN_POSITIONS_SIZE; i++){
            this.positions[i] = -1;
        }
        this.lock = new ReentrantLock();
    }


    public ReentrantLock getLock() {
        return lock;
    }


    public void put(long timestamp, long position){
        put(timestamp, position, null);
    }

    public void put(long timestamp, long position, Row row){
        int index = this.getIndex(getSec(timestamp));
        if(timestamp > this.latestTimestamp){
            this.latestTimestamp = timestamp;
            this.lastestRow = row;
        }
        if (this.oldestTimestamp == 0L || timestamp < this.oldestTimestamp){
            this.oldestTimestamp = timestamp;
        }
        this.positions[index] = position;
    }

    public long get(long timestamp) {
        return this.positions[getIndex(getSec(timestamp))];
    }

    public Row getLastestRow() {
        return lastestRow;
    }

    public long getLatestTimestamp(){
        return this.latestTimestamp;
    }

    public long getOldestTimestamp() {
        return oldestTimestamp;
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
            if ( t < end){
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
}
