package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Index {

    private final int vin;

    private final long[] positions;

    private long latestTimestamp;

    private long oldestTimestamp;

    private final Data data;

    private Row latestRow;

    private Block block;

    public Index(Data data, int vin){
        this.vin = vin;
        this.data = data;
        this.positions = new long[Const.TIME_SPAN];
        Arrays.fill(this.positions, -1);
    }

    public synchronized void insert(long timestamp, Map<String, ColumnValue> columns) throws IOException {
        if (block == null){
            block = new Block(data);
        }
        if (block.remaining() == 0){
            this.flush();
            block = new Block(data);
        }
        block.insert(timestamp, columns);
        this.insert(timestamp, -2);
    }

    public long get(long timestamp) {
        return this.positions[getIndex(timestamp)];
    }

    public void insert(long timestamp, long position){
        if(timestamp > latestTimestamp){
            latestTimestamp = timestamp;
        }
        if (oldestTimestamp == 0L || timestamp < oldestTimestamp){
            oldestTimestamp = timestamp;
        }
        positions[getIndex(timestamp)] = position;
    }

    public Map<String, ColumnValue> get(long timestamp, Set<String> requestedColumns) throws IOException {
        Map<Long, Map<String, ColumnValue>> results = range(timestamp, timestamp + 1, requestedColumns);
        return results.get(timestamp);
    }

    public Map<String, ColumnValue> getLatest(Set<String> requestedColumns) throws IOException {
        if (latestRow == null || latestRow.getTimestamp() != latestTimestamp) {
            Map<String, ColumnValue> columnValue = this.get(latestTimestamp, Const.EMPTY_COLUMNS);
            this.latestRow = new Row(null, latestTimestamp, columnValue);
        }
        if (requestedColumns.isEmpty() || requestedColumns.size() == this.latestRow.getColumns().size()){
            return this.latestRow.getColumns();
        }
        return this.latestRow.getColumns().entrySet().stream()
                .filter(entry -> requestedColumns.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Map<Long, Map<String, ColumnValue>> range(long start, long end, Set<String> requestedColumns) throws IOException {
        int left = getSec(Math.max(start, this.oldestTimestamp));
        int right = getSec(Math.min(end, this.latestTimestamp));
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

        if (requestedColumns.isEmpty()){
            requestedColumns = Const.COLUMNS_INDEX.keySet();
        }
        Map<Long, Map<String, ColumnValue>> results = new HashMap<>();
        for (Map.Entry<Long, Set<Long>> e: timestamps.entrySet()){
            long pos = e.getKey();
            Set<Long> requestedTimestamps = e.getValue();
            // read from memory
            if (pos == -2 && block != null){
                results.putAll(block.read(requestedTimestamps, requestedColumns));
                continue;
            }
            // read from disk
            results.putAll(Block.read(this.data, pos, requestedTimestamps, requestedColumns));
        }
        return results;
    }

    public void flush() throws IOException {
        if (block != null){
            long pos = block.flush();
            for (long ts: block.getTimestamps()){
                positions[getIndex(ts)] = pos;
            }
        }
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

    public int getVin() {
        return vin;
    }

    public int size(){
        int size = 0;
        for (long position: this.positions){
            if (position != -1){
                size++;
            }
        }
        return size;
    }
}
