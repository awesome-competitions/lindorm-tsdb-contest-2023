package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.util.Column;
import com.alibaba.lindorm.contest.util.Tuple;
import com.alibaba.lindorm.contest.v2.codec.Codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Block {

    private final long[] timestamps;

    private final ColumnValue[][] values;

    private final Data data;

    private int size;

    public Block(Data data){
        this.data = data;
        this.timestamps = new long[Const.BLOCK_SIZE];
        this.values = new ColumnValue[Const.BLOCK_SIZE][Const.COLUMNS.size()];
    }

    public int insert(long timestamp, Map<String, ColumnValue> columns){
        this.timestamps[size] = timestamp;
        for (int i = 0; i < Const.COLUMNS.size(); i ++){
            this.values[size][i] = columns.get(Const.COLUMNS.get(i));
        }
        return size ++;
    }

    public int remaining(){
        return Const.BLOCK_SIZE - size;
    }

    public void foreachTimestamps(BiConsumer<Integer, Long> consumer){
        for (int i = 0; i < size; i ++){
            consumer.accept(i, timestamps[i]);
        }
    }

    public void clear(){
        this.size = 0;
    }

    // 正序排序
    public void sort(){
        for (int i = 0; i < size; i ++){
            for (int j = i + 1; j < size; j ++){
                if (timestamps[i] > timestamps[j]){
                    long tmp = timestamps[i];
                    timestamps[i] = timestamps[j];
                    timestamps[j] = tmp;

                    ColumnValue[] tmpValues = values[i];
                    values[i] = values[j];
                    values[j] = tmpValues;
                }
            }
        }
    }

    public Header flush() throws IOException {
        this.sort();

        ByteBuffer writeBuffer = Context.getBlockWriteBuffer();
        writeBuffer.clear();

        int[] positions = new int[Const.COLUMNS.size()];
        double[] maxValues = new double[Const.COLUMNS.size()];
        double[] sumValues = new double[Const.COLUMNS.size()];
        for (int i = 0; i < Const.COLUMNS.size(); i ++){
            positions[i] = writeBuffer.position();
            double max = Long.MIN_VALUE;
            double sum = 0;

            String name = Const.COLUMNS.get(i);
            Column column = Const.COLUMNS_INDEX.get(name);
            switch (column.getType()){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    for (int k = 0; k < size; k ++){
                        ColumnValue value = values[k][i];
                        double doubleVal = value.getDoubleFloatValue();
                        sum += doubleVal;
                        if (doubleVal > max){
                            max = doubleVal;
                        }
                        writeBuffer.putDouble(doubleVal);
                    }
                    break;
                case COLUMN_TYPE_INTEGER:
                    Codec<int[]> codec = Const.COLUMNS_CODEC.getOrDefault(name, Const.DEFAULT_INT_CODEC);
                    int[] intValues = new int[size];
                    for (int k = 0; k < size; k ++){
                        ColumnValue value = values[k][i];
                        int intVal = value.getIntegerValue();
                        sum += intVal;
                        if (intVal > max){
                            max = intVal;
                        }
                        intValues[k] = intVal;
                    }
                    try{
                        codec.encode(writeBuffer, intValues);
                    }catch (Throwable e){
                        throw new RuntimeException(name + " encode err, " + Arrays.toString(intValues) + ", timestamps: " + Arrays.toString(timestamps), e);
                    }
                    break;
                case COLUMN_TYPE_STRING:
                    for (int k = 0; k < size; k ++){
                        ColumnValue value = values[k][i];
                        byte[] bs = value.getStringValue().array();
                        writeBuffer.put((byte) bs.length);
                        writeBuffer.put(bs);
                    }
                    break;
            }
            maxValues[i] = max;
            sumValues[i] = sum;
        }
        writeBuffer.flip();
        int length = writeBuffer.remaining();
        long pos = this.data.write(writeBuffer);
        return new Header(size, pos, length, positions, maxValues, sumValues);
    }

    public List<Tuple<Long, Map<String, ColumnValue>>> read(List<Tuple<Long, Integer>> requestedTimestamps, Collection<String> requestedColumns) {
        Tuple<Long, Map<String, ColumnValue>>[] results = new Tuple[requestedTimestamps.size()];
        for (String requestedColumn: requestedColumns){
            Column column = Const.COLUMNS_INDEX.get(requestedColumn);
            for(int i = 0; i < requestedTimestamps.size(); i ++){
                Tuple<Long, Integer> tuple = requestedTimestamps.get(i);
                long timestamp = tuple.K();
                int index = tuple.V();

                Tuple<Long, Map<String, ColumnValue>> result = results[i];
                if (result == null){
                    result = new Tuple<>(timestamp, new HashMap<>());
                    results[i] = result;
                }
                result.V().put(requestedColumn, this.values[index][column.getIndex()]);
            }
        }
        return Arrays.asList(results);
    }

    public void aggregate(List<Tuple<Long, Integer>> requestedTimestamps, String requestedColumn, Aggregator consumer) {
        Column column = Const.COLUMNS_INDEX.get(requestedColumn);
        ColumnValue.ColumnType type = column.getType();
        for (Tuple<Long, Integer> e: requestedTimestamps){
            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    consumer.accept(this.values[column.getIndex()][e.V()].getDoubleFloatValue());
                    break;
                case COLUMN_TYPE_INTEGER:
                    consumer.accept((double) this.values[column.getIndex()][e.V()].getIntegerValue());
                    break;
            }
        }
    }

    public static List<Tuple<Long, Map<String, ColumnValue>>> read(Data data, Header header, List<Tuple<Long, Integer>> requestedTimestamps, Collection<String> requestedColumns) throws IOException {
        ByteBuffer readBuffer = Context.getBlockReadBuffer();
        readBuffer.clear();

        int readBytes = data.read(readBuffer, header.position, header.length);
        if (readBytes != header.length){
            throw new IOException("read bytes not enough");
        }
        readBuffer.flip();

        int size = header.size;
        int[] positions = header.positions;

        Tuple<Long, Map<String, ColumnValue>>[] results = new Tuple[requestedTimestamps.size()];
        for (String requestedColumn: requestedColumns){
            Column column = Const.COLUMNS_INDEX.get(requestedColumn);
            int index = column.getIndex();
            ColumnValue.ColumnType type = column.getType();

            int latestPos = header.length;
            if (index < positions.length - 1){
                latestPos = positions[index + 1];
            }
            int currentPos = positions[index];

            readBuffer.clear();
            readBuffer.position(currentPos);
            readBuffer.limit(latestPos);

            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    double[] doubleValues = Context.getBlockDoubleValues();
                    for (int i = 0; i < size; i++) {
                        doubleValues[i] = readBuffer.getDouble();
                    }
                    for (int i = 0; i < requestedTimestamps.size(); i ++){
                        Tuple<Long, Integer> e = requestedTimestamps.get(i);
                        Tuple<Long, Map<String, ColumnValue>> result = results[i];
                        if (result == null){
                            result = new Tuple<>(e.K(), new HashMap<>());
                            results[i] = result;
                        }
                        result.V().put(requestedColumn, new ColumnValue.DoubleFloatColumn(doubleValues[e.V()]));
                    }
                    break;
                case COLUMN_TYPE_INTEGER:
                    Codec<int[]> codec = Const.COLUMNS_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_INT_CODEC);
                    try{
                        int[] intValues = codec.decode(readBuffer, size);
                        for (int i = 0; i < requestedTimestamps.size(); i ++){
                            Tuple<Long, Integer> e = requestedTimestamps.get(i);
                            Tuple<Long, Map<String, ColumnValue>> result = results[i];
                            if (result == null){
                                result = new Tuple<>(e.K(), new HashMap<>());
                                results[i] = result;
                            }
                            result.V().put(requestedColumn, new ColumnValue.IntegerColumn(intValues[e.V()]));
                        }
                    }catch (Throwable e){
                        throw new RuntimeException(requestedColumn + " decode err", e);
                    }
                    break;
                case COLUMN_TYPE_STRING:
                    ByteBuffer[] stringValues = Context.getBlockStringValues();
                    for (int i = 0; i < size; i++) {
                        ByteBuffer val = ByteBuffer.allocate(readBuffer.get());
                        readBuffer.get(val.array(), 0, val.limit());
                        stringValues[i] = val;
                    }
                    for (int i = 0; i < requestedTimestamps.size(); i ++){
                        Tuple<Long, Integer> e = requestedTimestamps.get(i);
                        Tuple<Long, Map<String, ColumnValue>> result = results[i];
                        if (result == null){
                            result = new Tuple<>(e.K(), new HashMap<>());
                            results[i] = result;
                        }
                        result.V().put(requestedColumn, new ColumnValue.StringColumn(stringValues[e.V()]));
                    }
                    break;
            }
        }
        return Arrays.asList(results);
    }

    public static void aggregate(Data data, Header header, List<Tuple<Long, Integer>> requestedTimestamps, String requestedColumn, Aggregator aggregator) throws IOException {
        int size = header.size;

        Column column = Const.COLUMNS_INDEX.get(requestedColumn);
        int index = column.getIndex();
        ColumnValue.ColumnType type = column.getType();

        if (aggregator.getColumnFilter() == null && requestedTimestamps.size() == size){
            // return first
            switch (aggregator.getAggregator()){
                case MAX:
                    aggregator.accept(header.maxValues[index]);
                    break;
                case AVG:
                    aggregator.accept(header.sumValues[index], size);
                    break;
            }
            return;
        }

        int latestPos = header.length;
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
                double[] doubleValues = Context.getBlockDoubleValues();
                for (int i = 0; i < size; i++) {
                    doubleValues[i] = readBuffer.getDouble();
                }
                for (Tuple<Long, Integer> e: requestedTimestamps){
                    aggregator.accept(doubleValues[e.V()]);
                }
                break;
            case COLUMN_TYPE_INTEGER:
                Codec<int[]> codec = Const.COLUMNS_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_INT_CODEC);
                try{
                    int[] intValues = codec.decode(readBuffer, size);
                    for (Tuple<Long, Integer> e: requestedTimestamps){
                        aggregator.accept((double) intValues[e.V()]);
                    }
                }catch (Throwable e){
                    throw new RuntimeException(requestedColumn + " decode err", e);
                }
                break;
        }
    }

    public static class Header {

        private final int size;

        private final long position;

        private final int length;

        private final int[] positions;

        private final double[] maxValues;

        private final double[] sumValues;

        public Header(int size, long position, int length, int[] positions, double[] maxValues, double[] sumValues) {
            this.size = size;
            this.position = position;
            this.length = length;
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

        public int getLength() {
            return length;
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
