package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.util.FilterMap;
import com.alibaba.lindorm.contest.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

public class Block {

    private final Map<Long, Map<String, ColumnValue>> values;

    private final int size;

    private final Data data;

    public Block(Data data){
        this.size = Const.BLOCK_SIZE;
        this.data = data;
        this.values = new HashMap<>();
    }

    public void insert(long timestamp, Map<String, ColumnValue> columns){
        values.put(timestamp, columns);
    }

    public int remaining(){
        return size - this.values.size() - 1;
    }

    public Set<Long> getTimestamps() {
        return values.keySet();
    }

    public long flush() throws IOException {
        ByteBuffer dataBuffer = Context.getBlockDataBuffer();
        dataBuffer.clear();

        ByteBuffer headerBuffer = Context.getBlockHeaderBuffer();
        headerBuffer.clear();

        Set<Long> timestamps = values.keySet();

        for (String columnKey: Const.SORTED_COLUMNS){
            headerBuffer.putInt(dataBuffer.position());
            for (Map.Entry<Long, Map<String, ColumnValue>> e: values.entrySet()){
                ColumnValue value = e.getValue().get(columnKey);
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
        writerBuffer.putInt(values.size());
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
        for (long requiredTimestamp: requestedTimestamps){
            Map<String, ColumnValue> columnValues = this.values.get(requiredTimestamp);
            results.put(requiredTimestamp, new FilterMap<>(columnValues, requestedColumns));
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
                        ByteBuffer val = ByteBuffer.allocate(readBuffer.get());
                        readBuffer.get(val.array(), 0, val.limit());
                        stringValues[i] = val;
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


    public void aggregate(Set<Long> requestedTimestamps, String requestedColumn, Consumer<Double> consumer) {
        Colum column = Const.COLUMNS_INDEX.get(requestedColumn);
        ColumnValue.ColumnType type = column.getType();
        for (long requiredTimestamp: requestedTimestamps){
            Map<String, ColumnValue> columnValues = this.values.get(requiredTimestamp);
            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    consumer.accept(columnValues.get(requestedColumn).getDoubleFloatValue());
                    break;
                case COLUMN_TYPE_INTEGER:
                    consumer.accept((double) columnValues.get(requestedColumn).getIntegerValue());
                    break;
            }
        }
    }

    public static void aggregate(Data data, long position, Set<Long> requestedTimestamps, String requestedColumn, Consumer<Double> consumer) throws IOException {
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

        byte[] requestedIndex = new byte[tsCount];
        for (int i = 0; i < tsCount; i++) {
            if (requestedTimestamps.contains(readBuffer.getLong())){
                requestedIndex[i] = 1;
            }
        }

        int readPos = readBuffer.position();
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
                for (int i = 0; i < tsCount; i++) {
                    double doubleValue = readBuffer.getDouble();
                    if (requestedIndex[i] == 0){
                        continue;
                    }
                    consumer.accept(doubleValue);
                }
                break;
            case COLUMN_TYPE_INTEGER:
                for (int i = 0; i < tsCount; i++) {
                    int intValue = readBuffer.getInt();
                    if (requestedIndex[i] == 0){
                        continue;
                    }
                    consumer.accept((double) intValue);
                }
                break;
        }
    }
}
