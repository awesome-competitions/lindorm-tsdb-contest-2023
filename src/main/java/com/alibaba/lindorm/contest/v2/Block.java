package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Block {

    // block size
    private final List<Long> timestamps;

    private final Map<String, List<ColumnValue>> values;

    private final int size;

    private final Data data;

    public Block(Data data){
        this.size = Const.BLOCK_SIZE;
        this.timestamps = new ArrayList<>(this.size);
        this.data = data;
        this.values = new ConcurrentHashMap<>(60);
    }

    public synchronized void insert(long timestamp, Map<String, ColumnValue> columns){
        this.timestamps.add(timestamp);
        for (Map.Entry<String, ColumnValue> e: columns.entrySet()){
            List<ColumnValue> values = this.values.computeIfAbsent(e.getKey(), k -> new ArrayList<>(this.size));
            values.add(e.getValue());
        }
    }

    public int remaining(){
        return size - this.timestamps.size() - 1;
    }

    public List<Long> getTimestamps() {
        return timestamps;
    }

    public long flush() throws IOException {
        ByteBuffer dataBuffer = Context.getBlockDataBuffer();
        dataBuffer.clear();

        ByteBuffer headerBuffer = Context.getBlockHeaderBuffer();
        headerBuffer.clear();

        for (String columnKey: Const.SORTED_COLUMNS){
            headerBuffer.putInt(dataBuffer.position());
            List<ColumnValue> values = this.values.get(columnKey);
            for (ColumnValue value: values){
                switch (value.getColumnType()){
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        dataBuffer.putDouble(value.getDoubleFloatValue());
                        break;
                    case COLUMN_TYPE_INTEGER:
                        dataBuffer.putInt(value.getIntegerValue());
                        break;
                    case COLUMN_TYPE_STRING:
                        byte[] bs = value.getStringValue().array();
                        dataBuffer.put((byte) bs.length);
                        dataBuffer.put(bs);
                        break;
                }
            }
        }

        for (long timestamp: timestamps){
            headerBuffer.putLong(timestamp);
        }

        headerBuffer.flip();
        dataBuffer.flip();

        ByteBuffer writerBuffer = Context.getBlockWriteBuffer();
        writerBuffer.clear();
        // header
        writerBuffer.putInt(timestamps.size());
        writerBuffer.putInt(dataBuffer.remaining());
        writerBuffer.put(headerBuffer);
        writerBuffer.put(dataBuffer);
        writerBuffer.flip();
        int size = writerBuffer.remaining();
        long pos = this.data.write(writerBuffer);
        return Util.assemblePosLen(size, pos);
    }

    public Map<Long, Map<String, ColumnValue>> read(Set<Long> requestedTimestamps, Set<String> requestedColumns) {
        Map<Long, Map<String, ColumnValue>> results = new HashMap<>();
        for (String requestedColumn: requestedColumns){
            List<ColumnValue> columnValues = this.values.get(requestedColumn);
            for (int i = 0; i < timestamps.size(); i++) {
                long timestamp = timestamps.get(i);
                if (! requestedTimestamps.contains(timestamp)){
                    continue;
                }
                Map<String, ColumnValue> values = results.computeIfAbsent(timestamp, k -> new HashMap<>());
                values.put(requestedColumn, columnValues.get(i));
            }
        }
        return results;
    }

    public static Map<Long, Map<String, ColumnValue>> read(Data data, long position, Set<Long> requestedTimestamps, Set<String> requestedColumns) throws IOException {
        ByteBuffer readBuffer = Context.getBlockReadBuffer();
        readBuffer.clear();

        int len = Util.parseLen(position);
        position = Util.parsePos(position);

        int readBytes = data.read(readBuffer, position, len);
        if (readBytes != len){
            throw new IOException("read bytes not enough");
        }
        readBuffer.flip();

        int tsCount = readBuffer.getInt();
        int dataSize = readBuffer.getInt();

        int[] positions = Context.getBlockPositions();
        for (int i = 0; i < positions.length; i++) {
            positions[i] = readBuffer.getInt();
        }

        long[] timestamps = Context.getBlockTimestamps();
        Map<Long, Integer> timestampIndex = new HashMap<>();
        for (int i = 0; i < tsCount; i++) {
            timestamps[i] = readBuffer.getLong();
            if (requestedTimestamps.contains(timestamps[i])){
                timestampIndex.put(timestamps[i], i);
            }
        }

        int readPos = readBuffer.position();
        Map<Long, Map<String, ColumnValue>> results = new HashMap<>();
        for (String requestedColumn: requestedColumns){
            Colum column = Const.COLUMNS_INDEX.get(requestedColumn);
            int index = column.getIndex();
            ColumnValue.ColumnType type = column.getType();

            int latestPos = dataSize;
            if (index < positions.length - 1){
                latestPos = positions[index + 1];
            }
            int currentPos = positions[index];

            readBuffer.clear();
            readBuffer.position(readPos + currentPos);
            readBuffer.limit(readPos + latestPos);

            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    double[] doubleValues = Context.getBlockDoubleValues();
                    for (int i = 0; i < tsCount; i++) {
                        doubleValues[i] = readBuffer.getDouble();
                    }
                    for (Map.Entry<Long, Integer> e: timestampIndex.entrySet()){
                        Map<String, ColumnValue> values = results.computeIfAbsent(e.getKey(), k -> new HashMap<>());
                        values.put(requestedColumn, new ColumnValue.DoubleFloatColumn(doubleValues[e.getValue()]));
                    }
                    break;
                case COLUMN_TYPE_INTEGER:
                    int[] intValues = Context.getBlockIntValues();
                    for (int i = 0; i < tsCount; i++) {
                        intValues[i] = readBuffer.getInt();
                    }
                    for (Map.Entry<Long, Integer> e: timestampIndex.entrySet()){
                        Map<String, ColumnValue> values = results.computeIfAbsent(e.getKey(), k -> new HashMap<>());
                        values.put(requestedColumn, new ColumnValue.IntegerColumn(intValues[e.getValue()]));
                    }
                    break;
                case COLUMN_TYPE_STRING:
                    ByteBuffer[] stringValues = Context.getBlockStringValues();
                    for (int i = 0; i < tsCount; i++) {
                        ByteBuffer val = stringValues[i];
                        val.clear();
                        val.limit(readBuffer.get());
                        readBuffer.put(val.array(), 0, val.limit());
                    }
                    for (Map.Entry<Long, Integer> e: timestampIndex.entrySet()){
                        Map<String, ColumnValue> values = results.computeIfAbsent(e.getKey(), k -> new HashMap<>());
                        values.put(requestedColumn, new ColumnValue.StringColumn(stringValues[e.getValue()]));
                    }
                    break;
            }
        }
        return results;
    }
}
