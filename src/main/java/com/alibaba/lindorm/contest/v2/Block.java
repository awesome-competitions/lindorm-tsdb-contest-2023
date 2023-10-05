package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Block {

    // block size
    private final long[] timestamps;

    private final Map<String, ColumnValue[]> values;

    private final int size;

    private final Data data;

    private int index;

    public Block(Data data){
        this.size = Const.BLOCK_SIZE;
        this.timestamps = new long[size];
        this.data = data;
        this.values = new ConcurrentHashMap<>(60);
    }

    public synchronized void insert(long timestamp, Map<String, ColumnValue> columns){
        timestamps[index] = timestamp;
        for (Map.Entry<String, ColumnValue> e: columns.entrySet()){
            ColumnValue[] values = this.values.computeIfAbsent(e.getKey(), k -> new ColumnValue[this.size]);
            values[index] = e.getValue();
        }
        index ++;
    }

    public int remaining(){
        return size - index - 1;
    }

    public long flush() throws IOException {
        ByteBuffer dataBuffer = Context.getBlockWriteBuffer();
        dataBuffer.clear();

        ByteBuffer posBuffer = Context.getBlockPositionBuffer();
        posBuffer.clear();

        posBuffer.putInt(dataBuffer.position());
        for (long timestamp: timestamps){
            dataBuffer.putLong(timestamp);
        }

        String[] columnNames = values.keySet().toArray(new String[]{});
        Arrays.sort(columnNames);

        for (String columnKey: columnNames){
            posBuffer.putInt(dataBuffer.position());
            ColumnValue[] values = this.values.get(columnKey);
            for (ColumnValue value: values){
                switch (value.getColumnType()){
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        dataBuffer.putDouble(value.getDoubleFloatValue());
                    case COLUMN_TYPE_INTEGER:
                        dataBuffer.putInt(value.getIntegerValue());
                    case COLUMN_TYPE_STRING:
                        dataBuffer.put(value.getStringValue().array());
                }
            }
        }

        posBuffer.flip();
        dataBuffer.flip();

        ByteBuffer writerBuffer = Context.getBlockWriteBuffer();
        writerBuffer.clear();
        writerBuffer.putInt(posBuffer.remaining() + dataBuffer.remaining());
        writerBuffer.put(posBuffer);
        writerBuffer.put(dataBuffer);

        return this.data.write(writerBuffer);
    }

}
