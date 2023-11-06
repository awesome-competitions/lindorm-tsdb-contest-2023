package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.util.Column;
import com.alibaba.lindorm.contest.util.Tuple;
import com.alibaba.lindorm.contest.v2.codec.StringCodec;
import com.alibaba.lindorm.contest.v2.codec.Codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;

public class Block {

    private final long[] timestamps;

    private final int[][] intValues;

    private final double[][] doubleValues;

    private final ByteBuffer[][] stringValues;

    private final Data data;

    private int size;

    private int flushSize;

    public Block(Data data){
        this.data = data;
        this.timestamps = new long[Const.BLOCK_SIZE];
        this.intValues = new int[Const.INT_COLUMN_COUNT][Const.BLOCK_SIZE];
        this.doubleValues = new double[Const.DOUBLE_COLUMN_COUNT][Const.BLOCK_SIZE];
        this.stringValues = new ByteBuffer[Const.STRING_COLUMN_COUNT][Const.BLOCK_SIZE];
    }

    public void insert(long timestamp, Map<String, ColumnValue> columns){
        this.timestamps[size] = timestamp;
        for (int i = 0; i < Const.INT_COLUMNS.size(); i ++){
            this.intValues[i][size] = columns.get(Const.INT_COLUMNS.get(i)).getIntegerValue();
        }
        for (int i = 0; i < Const.DOUBLE_COLUMNS.size(); i ++){
            this.doubleValues[i][size] = columns.get(Const.DOUBLE_COLUMNS.get(i)).getDoubleFloatValue();
        }
        for (int i = 0; i < Const.STRING_COLUMNS.size(); i ++){
            this.stringValues[i][size] = columns.get(Const.STRING_COLUMNS.get(i)).getStringValue();
        }
        this.size ++;
    }

    public int remaining(){
        return Const.BLOCK_SIZE - size;
    }

    public int getSize() {
        return size;
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

        for (int k = 0; k < Const.INT_COLUMN_COUNT; k ++){
            int tmpIntValue = intValues[k][i];
            intValues[k][i] = intValues[k][j];
            intValues[k][j] = tmpIntValue;
        }

        for (int k = 0; k < Const.DOUBLE_COLUMN_COUNT; k ++){
            double tmpDoubleValue = doubleValues[k][i];
            doubleValues[k][i] = doubleValues[k][j];
            doubleValues[k][j] = tmpDoubleValue;
        }

        for (int k = 0; k < Const.STRING_COLUMN_COUNT; k ++){
            ByteBuffer tmpStringValue = stringValues[k][i];
            stringValues[k][i] = stringValues[k][j];
            stringValues[k][j] = tmpStringValue;
        }
    }

    // 正序排序
    private void preFlush(){
        if (size == 0){
            return;
        }

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
        if (flushSize == 0){
            return null;
        }

        ByteBuffer writeBuffer = Context.getBlockWriteBuffer();
        writeBuffer.clear();

        int intCount = Const.INT_COLUMNS.size();
        int doubleCount = Const.DOUBLE_COLUMNS.size();
        int numberCount = intCount + doubleCount;
        int stringCount = Const.STRING_COLUMNS.size();
        int columnCount = numberCount + stringCount;

        int[] positions = new int[columnCount];
        double[] maxValues = new double[numberCount];
        double[] sumValues = new double[numberCount];

        // write int values
        for (int i = 0; i < intCount; i ++){
            positions[i] = writeBuffer.position();
            Codec<int[]> intCodec = Const.COLUMNS_INTEGER_CODEC.getOrDefault(Const.INT_COLUMNS.get(i), Const.DEFAULT_INT_CODEC);
            double max = Long.MIN_VALUE;
            double sum = 0;
            for (int v = 0; v < flushSize; v ++){
                int intVal = intValues[i][v];
                sum += intVal;
                if (intVal > max) max = intVal;
            }
            long oldPosition = writeBuffer.position();
            intCodec.encode(writeBuffer, intValues[i], flushSize);
            long newPosition = writeBuffer.position();
            Const.COLUMNS_SIZE[i] += (newPosition - oldPosition);

            maxValues[i] = max;
            sumValues[i] = sum;
        }

        // write double values
        int columnIndex = intCount;
        for (int i = 0; i < doubleCount; i ++){
            positions[i + columnIndex] = writeBuffer.position();
            Codec<double[]> doubleCodec = Const.COLUMNS_DOUBLE_CODEC.getOrDefault(Const.DOUBLE_COLUMNS.get(i), Const.DEFAULT_DOUBLE_CODEC);
            double max = Long.MIN_VALUE;
            double sum = 0;
            for (int v = 0; v < flushSize; v ++){
                double doubleVal = doubleValues[i][v];
                sum += doubleVal;
                if (doubleVal > max) max = doubleVal;
            }
            long oldPosition = writeBuffer.position();
            doubleCodec.encode(writeBuffer, doubleValues[i], flushSize);
            long newPosition = writeBuffer.position();
            Const.COLUMNS_SIZE[i+columnIndex] += (newPosition - oldPosition);

            maxValues[i + intCount] = max;
            sumValues[i + intCount] = sum;
        }

        // write string values
        columnIndex += doubleCount;
        for (int i = 0; i < Const.STRING_COLUMNS.size(); i ++){
            positions[i + columnIndex] = writeBuffer.position() ;
            Codec<ByteBuffer[]> stringCodec = Const.COLUMNS_STRING_CODEC.getOrDefault(Const.STRING_COLUMNS.get(i), Const.DEFAULT_STRING_CODEC);
            long oldPosition = writeBuffer.position();
            stringCodec.encode(writeBuffer, stringValues[i], flushSize);
            long newPosition = writeBuffer.position();
            Const.COLUMNS_SIZE[i + columnIndex] += (newPosition - oldPosition);
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
                int columnIndex = column.getIndex();
                switch(column.getType()){
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        values.put(requestedColumn, new ColumnValue.DoubleFloatColumn(this.doubleValues[columnIndex][i]));
                        break;
                    case COLUMN_TYPE_INTEGER:
                        values.put(requestedColumn, new ColumnValue.IntegerColumn(this.intValues[columnIndex][i]));
                        break;
                    case COLUMN_TYPE_STRING:
                        values.put(requestedColumn, new ColumnValue.StringColumn(this.stringValues[columnIndex][i]));
                        break;
                }
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
            int columnIndex = column.getIndex();
            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    consumer.accept(t, this.doubleValues[columnIndex][i]);
                    break;
                case COLUMN_TYPE_INTEGER:
                    consumer.accept(t, (double) this.intValues[columnIndex][i]);
                    break;
            }
        }
    }

    public static List<Tuple<Long, Map<String, ColumnValue>>> read(Data data, Header header, int start, int end, Collection<String> requestedColumns) throws IOException {
        int count = header.count;
        int size = header.size;
        int[] positions = header.positions;
        int intCount = Const.INT_COLUMNS.size();
        int doubleCount = Const.DOUBLE_COLUMNS.size();
        int numberCount = intCount + doubleCount;

        Tuple<Long, Map<String, ColumnValue>>[] results = new Tuple[end - start + 1];
        for (String requestedColumn: requestedColumns){
            Column column = Const.COLUMNS_INDEX.get(requestedColumn);
            int index = column.getIndex();
            ColumnValue.ColumnType type = column.getType();

            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    index += intCount;
                    break;
                case COLUMN_TYPE_STRING:
                    index += numberCount;
                    break;
            }

            int latestPos = size;
            if (index < positions.length - 1){
                latestPos = positions[index + 1];
            }
            int currentPos = positions[index];

            ByteBuffer readBuffer = Context.getBlockReadBuffer();
            readBuffer.clear();
            data.read(readBuffer, header.position + currentPos, latestPos - currentPos);
            readBuffer.flip();

            double[] doubleValues = Context.getBlockDoubleValues();
            int[] intValues = Context.getBlockIntValues();
            ByteBuffer[] stringValues = Context.getBlockStringValues();

            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    Codec<double[]> doubleCodec = Const.COLUMNS_DOUBLE_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_DOUBLE_CODEC);
                    doubleCodec.decode(readBuffer, doubleValues, count);
                    break;
                case COLUMN_TYPE_INTEGER:
                    Codec<int[]> intCodec = Const.COLUMNS_INTEGER_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_INT_CODEC);
                    intCodec.decode(readBuffer, intValues, count);
                    break;
                case COLUMN_TYPE_STRING:
                    Codec<ByteBuffer[]> stringCodec = Const.COLUMNS_STRING_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_STRING_CODEC);
                    stringCodec.decode(readBuffer, stringValues, count);
                    break;
            }

            for (int i = start; i <= end; i ++){
                long t = header.start + (long) i * Const.TIMESTAMP_INTERVAL;
                Tuple<Long, Map<String, ColumnValue>> result = results[i - start];
                if (result == null){
                    result = new Tuple<>(t, new HashMap<>());
                    results[i - start] = result;
                }
                switch (type){
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        result.V().put(requestedColumn, new ColumnValue.DoubleFloatColumn(doubleValues[i]));
                        break;
                    case COLUMN_TYPE_INTEGER:
                        result.V().put(requestedColumn, new ColumnValue.IntegerColumn(intValues[i]));
                        break;
                    case COLUMN_TYPE_STRING:
                        result.V().put(requestedColumn, new ColumnValue.StringColumn(stringValues[i]));
                        break;
                }
            }
        }
        return Arrays.asList(results);
    }

    public static void aggregate(Data data, Header header, int start, int end, String requestedColumn, Aggregator aggregator) throws IOException {
        Column column = Const.COLUMNS_INDEX.get(requestedColumn);
        int index = column.getIndex();
        ColumnValue.ColumnType type = column.getType();

        if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)){
            index += Const.INT_COLUMNS.size();
        }

        int count = header.count;
        int requestedCount = end - start + 1;
        boolean isDownsample = aggregator instanceof Downsample;
        if (!isDownsample && aggregator.getColumnFilter() == null && requestedCount == count){
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

        ByteBuffer readBuffer = Context.getBlockReadBuffer();
        readBuffer.clear();
        data.read(readBuffer, header.position + currentPos, latestPos - currentPos);
        readBuffer.flip();

        double[] doubleValues = Context.getBlockDoubleValues();
        int[] intValues = Context.getBlockIntValues();

        switch (type){
            case COLUMN_TYPE_DOUBLE_FLOAT:
                Codec<double[]> doubleCodec = Const.COLUMNS_DOUBLE_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_DOUBLE_CODEC);
                doubleCodec.decode(readBuffer, doubleValues, count);
                for (int i = start; i <= end; i ++){
                    long t = header.start + (long) i * Const.TIMESTAMP_INTERVAL;
                    aggregator.accept(t, doubleValues[i]);
                }
                break;
            case COLUMN_TYPE_INTEGER:
                Codec<int[]> intCodec = Const.COLUMNS_INTEGER_CODEC.getOrDefault(requestedColumn, Const.DEFAULT_INT_CODEC);
                intCodec.decode(readBuffer, intValues, count);
                for (int i = start; i <= end; i ++){
                    long t = header.start + (long) i * Const.TIMESTAMP_INTERVAL;
                    aggregator.accept(t, (double) intValues[i]);
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
