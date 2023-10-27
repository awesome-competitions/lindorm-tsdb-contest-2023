package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.util.FilterMap;
import com.alibaba.lindorm.contest.util.Tuple;
import com.alibaba.lindorm.contest.util.Util;

import java.io.IOException;
import java.util.*;

public class Index {

    private final int vin;

    private final List<Block.Header> headers;

    private long latestTimestamp;

    private long oldestTimestamp;

    private final Data data;

    private Row latestRow;

    private Block block;

    public Index(Data data, int vin){
        this.vin = vin;
        this.data = data;
        this.headers = new ArrayList<>(Const.TIME_SPAN / Const.BLOCK_SIZE);
    }

    public synchronized void insert(long timestamp, Map<String, ColumnValue> columns) throws IOException {
        if (block == null){
            block = new Block(data);
        }
        if (block.remaining() == 0){
            this.flush();
        }
        block.insert(timestamp, columns);
        mark(timestamp);
    }

    public void mark(long timestamp){
        if(timestamp > latestTimestamp){
            latestTimestamp = timestamp;
        }
        if (oldestTimestamp == 0L || timestamp < oldestTimestamp){
            oldestTimestamp = timestamp;
        }
    }

    public Map<String, ColumnValue> get(long timestamp, Set<String> requestedColumns) throws IOException {
        List<Tuple<Long, Map<String, ColumnValue>>> results = range(timestamp, timestamp + 1, requestedColumns);
        if (results.isEmpty()){
            return Collections.emptyMap();
        }
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
        List<Tuple<Long, Map<String, ColumnValue>>> results = new ArrayList<>();
        this.searchTimestamps(start, end, (header, startIndex, endIndex) ->
            // read from disk
            results.addAll(Block.read(this.data, header, startIndex, endIndex, requestedColumns.isEmpty() ? Const.COLUMNS : requestedColumns))
        );
        // read from memory
        if (block != null){
            results.addAll(block.read(start, end, requestedColumns.isEmpty() ? Const.COLUMNS : requestedColumns));
        }
        return results;
    }

    public void aggregate(long start, long end, String requestedColumn, Aggregator aggregator) throws IOException {
        this.searchTimestamps(start, end, (header, startIndex, endIndex) ->
            // read from disk
            Block.aggregate(this.data, header, startIndex, endIndex, requestedColumn, aggregator)
        );
        // read from memory
        if (block != null){
            block.aggregate(start, end, requestedColumn, aggregator);
        }
    }

    // binary search
    public int searchStartBlock(long t) {
        if (headers.isEmpty()){
            return -1;
        }
        int left = 0;
        int right = headers.size() - 1;
        while (true){
            int mid = (left + right) / 2;
            Block.Header header = headers.get(mid);
            if (t < header.getStart()){
                right = mid - 1;
            } else if (t >= header.getStart() && t <= header.getEnd()){
                return mid;
            } else  {
                left = mid + 1;
            }
            if (left > right){
               return -1;
            }
        }
    }

    public void searchTimestamps(long start, long end, ThConsumer<Block.Header, Integer, Integer> consumer) throws IOException {
        // binary search
        int target = searchStartBlock(start);
        if (target == -1){
            return;
        }
        for (int i = target; i < headers.size(); i++) {
            Block.Header header = headers.get(i);
            if (header.getStart() > end){
                break;
            }
            long startTs = Util.trim(Math.max(start, header.getStart()));
            if (startTs < start) startTs += Const.TIMESTAMP_INTERVAL;
            long endTs = Util.trim(Math.min(end-1, header.getEnd()));
            int startIndex = (int) ((startTs - header.getStart()) / Const.TIMESTAMP_INTERVAL);
            int endIndex = (int) ((endTs - header.getStart()) / Const.TIMESTAMP_INTERVAL);
            consumer.accept(header, startIndex, endIndex);
        }
    }

    public Block.Header flush() throws IOException {
        if (block == null){
            return null;
        }
        Block.Header header = block.flush();
        this.headers.add(header);
        return header;
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

    public List<Block.Header> getHeaders() {
        return headers;
    }

    public int getVin() {
        return vin;
    }
}
