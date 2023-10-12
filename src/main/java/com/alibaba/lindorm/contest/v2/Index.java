package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.util.FilterMap;
import com.alibaba.lindorm.contest.util.Tuple;
import com.alibaba.lindorm.contest.util.Util;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

public class Index {

    private final int vin;

    private final long[] positions;

    private final Map<Long, Block.Header> headers;

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
        this.headers = new HashMap<>(Const.TIME_SPAN / Const.BLOCK_SIZE);
    }

    public synchronized void insert(long timestamp, Map<String, ColumnValue> columns) throws IOException {
        if (block == null){
            block = new Block(data);
        }
        if (block.remaining() == 0){
            this.flush();
        }
        int index = block.insert(timestamp, columns);
        this.mark(timestamp, Util.assemblePosIndex(index, -2));
    }

    public long get(long timestamp) {
        return this.positions[getIndex(timestamp)];
    }

    public void mark(long timestamp, long position){
        if(timestamp > latestTimestamp){
            latestTimestamp = timestamp;
        }
        if (oldestTimestamp == 0L || timestamp < oldestTimestamp){
            oldestTimestamp = timestamp;
        }
        positions[getIndex(timestamp)] = position;
    }

    public Map<String, ColumnValue> get(long timestamp, Set<String> requestedColumns) throws IOException {
        List<Tuple<Long, Map<String, ColumnValue>>> results = range(timestamp, timestamp + 1, requestedColumns);
        return results.get(0).V();
    }

    public Map<String, ColumnValue> getLatest(Set<String> requestedColumns) throws IOException {
        if (latestRow == null || latestRow.getTimestamp() != latestTimestamp) {
            Map<String, ColumnValue> columnValue = this.get(latestTimestamp, Const.EMPTY_COLUMNS);
            this.latestRow = new Row(null, latestTimestamp, columnValue);
        }
        return new FilterMap<>(this.latestRow.getColumns(), requestedColumns);
    }

    public List<Tuple<Long, Map<String, ColumnValue>>> range(long start, long end, Collection<String> requestedColumns) throws IOException {
        if (requestedColumns.isEmpty()){
            requestedColumns = Const.COLUMNS;
        }
        Map<Long, List<Tuple<Long, Integer>>> timestamps = searchTimestamps(start, end);
        List<Tuple<Long, Map<String, ColumnValue>>> results = new ArrayList<>();
        for (Map.Entry<Long, List<Tuple<Long, Integer>>> e: timestamps.entrySet()){
            long pos = e.getKey();
            List<Tuple<Long, Integer>> requestedTimestamps = e.getValue();
            // read from memory
            if (pos == -2 && block != null){
                results.addAll(block.read(requestedTimestamps, requestedColumns));
                continue;
            }
            // read from disk
            results.addAll(Block.read(this.data, headers.get(pos), requestedTimestamps, requestedColumns));
        }
        return results;
    }

    public void aggregate(long start, long end, String requestedColumn, Consumer<Double> consumer) throws IOException {
        Map<Long, List<Tuple<Long, Integer>>> timestamps = searchTimestamps(start, end);
        for (Map.Entry<Long, List<Tuple<Long, Integer>>> e: timestamps.entrySet()){
            long pos = e.getKey();
            List<Tuple<Long, Integer>> requestedTimestamps = e.getValue();
            // read from memory
            if (pos == -2 && block != null){
                block.aggregate(requestedTimestamps, requestedColumn, consumer);
                continue;
            }
            // read from disk
            Block.aggregate(this.data, headers.get(pos), requestedTimestamps, requestedColumn, consumer);
        }
    }

    public Map<Long, List<Tuple<Long, Integer>>> searchTimestamps(long start, long end) {
        int left = getSec(Math.max(start, this.oldestTimestamp));
        int right = getSec(Math.min(end, this.latestTimestamp));
        Map<Long, List<Tuple<Long, Integer>>> timestamps = new HashMap<>();
        for (int i = left; i <= right; i++) {
            int index = getIndex(i);
            long pos = this.positions[index];
            if (pos == -1){
                continue;
            }
            long t = i * 1000L;
            if (t < end && t >= start){
                timestamps.computeIfAbsent(Util.parsePos(pos), k -> new ArrayList<>())
                        .add(new Tuple<>(t, Util.parseIndex(pos)));
            }
        }
        return timestamps;
    }

    public void flush() throws IOException {
        if (block == null){
            return;
        }
        Block.Header header = block.flush();
        headers.put(header.getPosition(), header);
        block.foreachTimestamps((index, ts) -> positions[getIndex(ts)] = Util.assemblePosIndex(index, header.getPosition()));
        block.clear();
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

    public Map<Long, Block.Header> getHeaders() {
        return headers;
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
