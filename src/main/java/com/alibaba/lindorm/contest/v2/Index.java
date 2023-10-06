package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.v2.function.ThConsumer;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;

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
        Arrays.fill(this.positions, -1);
    }

    public void insert(long timestamp, Map<String, ColumnValue> columns) throws IOException {
        if(timestamp > latestTimestamp){
            latestTimestamp = timestamp;
        }
        if (oldestTimestamp == 0L || timestamp < oldestTimestamp){
            oldestTimestamp = timestamp;
        }

        int index = getIndex(timestamp);
        if (block == null){
            block = new Block(data);
        }
        if (block.remaining() == 0){
            positions[index] = block.flush();
            block = new Block(data);
        }
        block.insert(timestamp, columns);
    }

    public Map<String, ColumnValue> get(long timestamp, Set<String> requestedColumns) throws IOException {
        Map<Long, Map<String, ColumnValue>> results = range(timestamp, timestamp + 1, requestedColumns);
        return results.get(timestamp);
    }

    public Map<Long, Map<String, ColumnValue>> range(long start, long end, Set<String> requestedColumns) throws IOException {
        int left = getIndex(Math.max(start, this.oldestTimestamp));
        int right = getIndex(Math.min(end, this.latestTimestamp));
        Map<Long, Set<Long>> timestamps = new HashMap<>();
        for (int i = left; i <= right; i++) {
            int index = getIndex(i);
            long pos = this.positions[index];
            if (pos == -1){
                continue;
            }
            long t = i * 1000L;
            if (t < end && t >= start){
                timestamps.computeIfAbsent(pos, k -> new HashSet<>()).add(t);
            }
        }

        Map<Long, Map<String, ColumnValue>> results = new HashMap<>();
        for (Map.Entry<Long, Set<Long>> e: timestamps.entrySet()){
            // read from disk
            results.putAll(Block.read(this.data, e.getKey(), e.getValue(), requestedColumns));
            // read from memory
            if (block != null){
                results.putAll(block.read(e.getValue(), requestedColumns));
            }
        }
        return results;
    }

    public long getLatestTimestamp() {
        return latestTimestamp;
    }

    public long getOldestTimestamp() {
        return oldestTimestamp;
    }

    private static int getSec(long timestamp){
        return (int) (timestamp/1000);
    }

    private static int getIndex(long timestamp){
        return getSec(timestamp) % Const.TIME_SPAN;
    }

    private static int getIndex(int seconds){
        return seconds % Const.TIME_SPAN;
    }
}
