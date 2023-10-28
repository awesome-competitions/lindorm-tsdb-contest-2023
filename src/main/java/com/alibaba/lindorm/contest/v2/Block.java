package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.util.Column;
import com.alibaba.lindorm.contest.util.Tuple;
import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.v2.codec.Codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;

public class Block {

    private final long[] timestamps;

    private final ColumnValue[][] values;

    private final Data data;

    private int size;

    private int flushSize;

    public Block(Data data){
        this.data = data;
        this.timestamps = new long[Const.BLOCK_SIZE];
        this.values = new ColumnValue[Const.BLOCK_SIZE][Const.COLUMNS.size()];
    }

    public void insert(long timestamp, Map<String, ColumnValue> columns){
        this.timestamps[size] = timestamp;
        for (int i = 0; i < Const.COLUMNS.size(); i ++){
            this.values[size][i] = columns.get(Const.COLUMNS.get(i));
        }
        this.size ++;
    }

    public int remaining(){
        return Const.BLOCK_SIZE - size;
    }

    public void clear(){
        int surplusSize = size - flushSize;
        for (int i = 0; i < surplusSize; i ++){
            swap(i, i + flushSize);
        }
        this.size = surplusSize;
        this.flushSize = 0;
    }

    private void swap(int i, int j){
        long tmp = timestamps[i];
        timestamps[i] = timestamps[j];
        timestamps[j] = tmp;

        ColumnValue[] tmpValues = values[i];
        values[i] = values[j];
        values[j] = tmpValues;
    }

    // 正序排序
    private void preFlush(){
        for (int i = 0; i < size; i ++){
            for (int j = i + 1; j < size; j ++){
                if (timestamps[i] > timestamps[j]){
                    swap(i, j);
                }
            }
        }

        this.flushSize = 1;
        for (int i = 1; i < size; i ++){
           if (timestamps[i] - timestamps[i - 1] > 1000){
               break;
           }
           this.flushSize ++;
        }
    }

    public Header flush() throws IOException {
        this.preFlush();

        ByteBuffer writeBuffer = Context.getBlockWriteBuffer();
        writeBuffer.clear();

        int[] positions = new int[Const.COLUMNS.size()];
        double[] maxValues = new double[Const.COLUMNS.size()];
        double[] sumValues = new double[Const.COLUMNS.size()];

        double[] doubleValues = new double[flushSize];
        int[] intValues = new int[flushSize];
        ByteBuffer[] stringValues = new ByteBuffer[flushSize];
        for (int i = 0; i < Const.COLUMNS.size(); i ++){
            positions[i] = writeBuffer.position();
            double max = Long.MIN_VALUE;
            double sum = 0;

            String name = Const.COLUMNS.get(i);
            Column column = Const.COLUMNS_INDEX.get(name);
            long oldPosition = writeBuffer.position();
            switch (column.getType()){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    Codec<double[]> doubleCodec = Const.COLUMNS_DOUBLE_CODEC.getOrDefault(name, Const.DEFAULT_DOUBLE_CODEC);
                    for (int k = 0; k < flushSize; k ++){
                        ColumnValue value = values[k][i];
                        double doubleVal = value.getDoubleFloatValue();
                        sum += doubleVal;
                        if (doubleVal > max){
                            max = doubleVal;
                        }
                        doubleValues[k] = doubleVal;
                    }
                    doubleCodec.encode(writeBuffer, doubleValues);
                    break;
                case COLUMN_TYPE_INTEGER:
                    Codec<int[]> intCodec = Const.COLUMNS_INTEGER_CODEC.getOrDefault(name, Const.DEFAULT_INT_CODEC);
                    for (int k = 0; k < flushSize; k ++){
                        ColumnValue value = values[k][i];
                        int intVal = value.getIntegerValue();
                        sum += intVal;
                        if (intVal > max){
                            max = intVal;
                        }
                        intValues[k] = intVal;
                    }
                    intCodec.encode(writeBuffer, intValues);
                    break;
                case COLUMN_TYPE_STRING:
                    Codec<ByteBuffer[]> stringCodec = Const.COLUMNS_STRING_CODEC.getOrDefault(name, Const.DEFAULT_STRING_CODEC);
                    for (int k = 0; k < flushSize; k ++){
                        stringValues[k] = values[k][i].getStringValue();
                    }
                    stringCodec.encode(writeBuffer, stringValues);
                    break;
            }
            long newPosition = writeBuffer.position();
            Const.COLUMNS_SIZE[i] += (newPosition - oldPosition);
            maxValues[i] = max;
            sumValues[i] = sum;
        }
        writeBuffer.flip();
        int size = writeBuffer.remaining();
        long pos = this.data.write(writeBuffer);
        Header header = new Header(size, flushSize, pos, timestamps[0], positions, maxValues, sumValues);
        this.clear();
        return header;
    }

    public List<Tuple<Long, Map<String, ColumnValue>>> read(long start, long end, Collection<String> requestedColumns) {
        List<Tuple<Long, Map<String, ColumnValue>>> results = new ArrayList<>();
        for(int i = 0; i < size; i ++){
            long t = timestamps[i];
            if (t >= end || t < start){
                continue;
            }
            Map<String, ColumnValue> values = new HashMap<>();
            for (String requestedColumn: requestedColumns){
                Column column = Const.COLUMNS_INDEX.get(requestedColumn);
                values.put(requestedColumn, this.values[i][column.getIndex()]);
            }
            results.add(new Tuple<>(t, values));
        }
        return results;
    }

    public void aggregate(long start, long end, String requestedColumn, Aggregator consumer) {
        Column column = Const.COLUMNS_INDEX.get(requestedColumn);
        ColumnValue.ColumnType type = column.getType();
        for(int i = 0; i < size; i ++){
            long t = timestamps[i];
            if (t >= end || t < start){
                continue;
            }
            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    consumer.accept(this.values[i][column.getIndex()].getDoubleFloatValue());
                    break;
                case COLUMN_TYPE_INTEGER:
                    consumer.accept((double) this.values[i][column.getIndex()].getIntegerValue());
                    break;
            }
        }
    }

    public static List<Tuple<Long, Map<String, ColumnValue>>> read(Data data, Header header, int start, int end, Collection<String> requestedColumns) throws IOException {
        ByteBuffer readBuffer = Context.getBlockReadBuffer();
        readBuffer.clear();

        int readBytes = data.read(readBuffer, header.position, header.size);
        if (readBytes != header.size){
            throw new IOException("read bytes not enough");
        }
        readBuffer.flip();

        int count = header.count;
        int[] positions = header.positions;

        Tuple<Long, Map<String, ColumnValue>>[] results = new Tuple[end - start + 1];
        for (String requestedColumn: requestedColumns){
            Column column = Const.COLUMNS_INDEX.get(requestedColumn);
            int index = column.getIndex();
            ColumnValue.ColumnType type = column.getType();

            int latestPos = header.size;
            if (index < positions.length - 1){
                latestPos = positions[index + 1];
            }
            int currentPos = positions[index];

            readBuffer.clear();
            readBuffer.position(currentPos);
            readBuffer.limit(latestPos);

            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    Codec<double[]> doubleCodec = Const.COLUMNS_DOUBLE_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_DOUBLE_CODEC);
                    double[] doubleValues = doubleCodec.decode(readBuffer, count);
                    for (int i = start; i <= end; i ++){
                        long t = header.start + (long) i * Const.TIMESTAMP_INTERVAL;
                        Tuple<Long, Map<String, ColumnValue>> result = results[i - start];
                        if (result == null){
                            result = new Tuple<>(t, new HashMap<>());
                            results[i - start] = result;
                        }
                        result.V().put(requestedColumn, new ColumnValue.DoubleFloatColumn(doubleValues[i]));
                    }
                    break;
                case COLUMN_TYPE_INTEGER:
                    Codec<int[]> intCodec = Const.COLUMNS_INTEGER_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_INT_CODEC);
                    int[] intValues = intCodec.decode(readBuffer, count);
                    for (int i = start; i <= end; i ++){
                        long t = header.start + (long) i * Const.TIMESTAMP_INTERVAL;
                        Tuple<Long, Map<String, ColumnValue>> result = results[i - start];
                        if (result == null){
                            result = new Tuple<>(t, new HashMap<>());
                            results[i - start] = result;
                        }
                        result.V().put(requestedColumn, new ColumnValue.IntegerColumn(intValues[i]));
                    }
                    break;
                case COLUMN_TYPE_STRING:
                    Codec<ByteBuffer[]> stringCodec = Const.COLUMNS_STRING_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_STRING_CODEC);
                    ByteBuffer[] stringValues = stringCodec.decode(readBuffer, count);
                    for (int i = start; i <= end; i ++){
                        long t = header.start + (long) i * Const.TIMESTAMP_INTERVAL;
                        Tuple<Long, Map<String, ColumnValue>> result = results[i - start];
                        if (result == null){
                            result = new Tuple<>(t, new HashMap<>());
                            results[i - start] = result;
                        }
                        result.V().put(requestedColumn, new ColumnValue.StringColumn(stringValues[i]));
                    }
                    break;
            }
        }
        return Arrays.asList(results);
    }

    public static void aggregate(Data data, Header header, int start, int end, String requestedColumn, Aggregator aggregator) throws IOException {
        Column column = Const.COLUMNS_INDEX.get(requestedColumn);
        int index = column.getIndex();
        ColumnValue.ColumnType type = column.getType();

        int count = header.count;
        int requestedCount = end - start + 1;
        if (aggregator.getColumnFilter() == null && requestedCount == count){
            // return first
            switch (aggregator.getAggregator()){
                case MAX:
                    aggregator.accept(header.maxValues[index]);
                    break;
                case AVG:
                    aggregator.accept(header.sumValues[index], count);
                    break;
            }
            return;
        }

        int latestPos = header.size;
        if (index < header.positions.length - 1){
            latestPos = header.positions[index + 1];
        }
        int currentPos = header.positions[index];
        int columnDataSize = latestPos - currentPos;

        ByteBuffer readBuffer = Context.getBlockReadBuffer();
        readBuffer.clear();
        int readBytes = data.read(readBuffer, header.position + currentPos, columnDataSize);
        if (readBytes != columnDataSize){
            throw new IOException("read bytes not enough");
        }
        readBuffer.flip();

        switch (type){
            case COLUMN_TYPE_DOUBLE_FLOAT:
                Codec<double[]> doubleCodec = Const.COLUMNS_DOUBLE_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_DOUBLE_CODEC);
                try{
                    double[] doubleValues = doubleCodec.decode(readBuffer, count);
                    for (int i = start; i <= end; i ++){
                        aggregator.accept(doubleValues[i]);
                    }
                }catch (Throwable e){
                    throw new RuntimeException(requestedColumn + " decode err", e);
                }
                break;
            case COLUMN_TYPE_INTEGER:
                Codec<int[]> intCodec = Const.COLUMNS_INTEGER_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_INT_CODEC);
                try{
                    int[] intValues = intCodec.decode(readBuffer, count);
                    for (int i = start; i <= end; i ++){
                        aggregator.accept((double) intValues[i]);
                    }
                }catch (Throwable e){
                    throw new RuntimeException(requestedColumn + " decode err", e);
                }
                break;
        }
    }

    public static class Header {

        private final int size;

        private final int count;

        private final long position;

        private final long start;

        private final int[] positions;

        private final double[] maxValues;

        private final double[] sumValues;

        public Header(int size, int count, long position, long start, int[] positions, double[] maxValues, double[] sumValues) {
            this.size = size;
            this.count = count;
            this.position = position;
            this.start = start;
            this.positions = positions;
            this.maxValues = maxValues;
            this.sumValues = sumValues;
        }

        public int getSize() {
            return size;
        }

        public long getPosition() {
            return position;
        }

        public int getCount() {
            return count;
        }

        public long getStart() {
            return start;
        }

        public long getEnd() {
            return start + (long) (count - 1) * Const.TIMESTAMP_INTERVAL;
        }

        public int[] getPositions() {
            return positions;
        }

        public double[] getMaxValues() {
            return maxValues;
        }

        public double[] getSumValues() {
            return sumValues;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Header header = (Header) o;
            return position == header.position;
        }

        @Override
        public int hashCode() {
            return Objects.hash(position);
        }
    }
}
