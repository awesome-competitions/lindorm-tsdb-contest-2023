package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Vin;
import com.alibaba.lindorm.contest.util.Column;
import com.alibaba.lindorm.contest.util.ColumnMap;
import com.alibaba.lindorm.contest.v2.codec.Codec;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

public class Block {

    private final long[] timestamps;

    private final int[][] intValues;

    private final double[][] doubleValues;

    private final ByteBuffer[][] stringValues;

    private int size;

    private int flushSize;

    public Block(){
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

        int intCount = Const.INT_COLUMNS.size();
        int doubleCount = Const.DOUBLE_COLUMNS.size();
        int numberCount = intCount + doubleCount;
        int stringCount = Const.STRING_COLUMNS.size();
        int columnCount = numberCount + stringCount;

        long[] positions = new long[columnCount];
        int[] lengths = new int[columnCount];
        double[] maxValues = new double[numberCount];
        double[] sumValues = new double[numberCount];

        // write int values
        for (int i = 0; i < intCount; i ++){
            ByteBuffer writeBuffer = Context.getBlockWriteBuffer();
            writeBuffer.clear();

            String columnName = Const.INT_COLUMNS.get(i);
            Codec<int[]> intCodec = Const.COLUMNS_INTEGER_CODEC.getOrDefault(columnName, Const.DEFAULT_INT_CODEC);
            double max = Long.MIN_VALUE;
            double sum = 0;
            for (int v = 0; v < flushSize; v ++){
                int intVal = intValues[i][v];
                sum += intVal;
                if (intVal > max) max = intVal;
            }
            intCodec.encode(writeBuffer, intValues[i], flushSize);
            writeBuffer.flip();
            lengths[i] = writeBuffer.remaining();

            maxValues[i] = max;
            sumValues[i] = sum;

            Column column = Const.COLUMNS_INDEX.get(columnName);
            positions[i] = column.getData().write(writeBuffer);
        }

        // write double values
        int columnIndex = intCount;
        for (int i = 0; i < doubleCount; i ++){
            ByteBuffer writeBuffer = Context.getBlockWriteBuffer();
            writeBuffer.clear();

            String columnName = Const.DOUBLE_COLUMNS.get(i);
            Codec<double[]> doubleCodec = Const.COLUMNS_DOUBLE_CODEC.getOrDefault(columnName, Const.DEFAULT_DOUBLE_CODEC);
            double max = Long.MIN_VALUE;
            double sum = 0;
            for (int v = 0; v < flushSize; v ++){
                double doubleVal = doubleValues[i][v];
                sum += doubleVal;
                if (doubleVal > max) max = doubleVal;
            }
            doubleCodec.encode(writeBuffer, doubleValues[i], flushSize);
            writeBuffer.flip();
            lengths[i + columnIndex] = writeBuffer.remaining();

            maxValues[i + columnIndex] = max;
            sumValues[i + columnIndex] = sum;

            Column column = Const.COLUMNS_INDEX.get(columnName);
            positions[i + columnIndex] = column.getData().write(writeBuffer);
        }

        // write string values
        columnIndex += doubleCount;
        for (int i = 0; i < Const.STRING_COLUMNS.size(); i ++){
            ByteBuffer writeBuffer = Context.getBlockWriteBuffer();
            writeBuffer.clear();

            String columnName = Const.STRING_COLUMNS.get(i);
            Codec<ByteBuffer[]> stringCodec = Const.COLUMNS_STRING_CODEC.getOrDefault(columnName, Const.DEFAULT_STRING_CODEC);
            stringCodec.encode(writeBuffer, stringValues[i], flushSize);
            writeBuffer.flip();
            lengths[i + columnIndex] = writeBuffer.remaining();

            Column column = Const.COLUMNS_INDEX.get(columnName);
            positions[i + columnIndex] = column.getData().write(writeBuffer);
        }

        Header header = new Header(flushSize, timestamps[0], positions, lengths, maxValues, sumValues);
        this.clear();
        return header;
    }

    public List<Row> read(Vin vin, long start, long end, Collection<String> requestedColumns) {
        List<Row> results = new ArrayList<>();
        for(int i = 0; i < size; i ++){
            long t = timestamps[i];
            if (t >= end || t < start){
                continue;
            }
            Map<String, ColumnValue> values = new ColumnMap(requestedColumns, Const.ALL_COLUMNS.size());
            for (String requestedColumn: requestedColumns){
                Column column = Const.COLUMNS_INDEX.get(requestedColumn);
                int columnIndex = column.getIndex();
                switch(column.getType()){
                    case COLUMN_TYPE_DOUBLE_FLOAT:
                        columnIndex -= Const.INT_COLUMNS.size();
                        values.put(requestedColumn, new ColumnValue.DoubleFloatColumn(this.doubleValues[columnIndex][i]));
                        break;
                    case COLUMN_TYPE_INTEGER:
                        values.put(requestedColumn, new ColumnValue.IntegerColumn(this.intValues[columnIndex][i]));
                        break;
                    case COLUMN_TYPE_STRING:
                        columnIndex -= Const.INT_COLUMNS.size() + Const.DOUBLE_COLUMNS.size();
                        values.put(requestedColumn, new ColumnValue.StringColumn(this.stringValues[columnIndex][i]));
                        break;
                }
            }
            results.add(new Row(vin, t, values));
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
                    columnIndex -= Const.INT_COLUMNS.size();
                    consumer.accept(t, this.doubleValues[columnIndex][i]);
                    break;
                case COLUMN_TYPE_INTEGER:
                    consumer.accept(t, (double) this.intValues[columnIndex][i]);
                    break;
            }
        }
    }

    public static List<Row> read(Vin vin, Header header, int start, int end, Collection<String> requestedColumns) throws IOException {
        Row[] results = new Row[end - start + 1];
        for (int i = start; i <= end; i ++){
            long t = header.start + (long) i * Const.TIMESTAMP_INTERVAL;
            results[i - start] = new Row(vin, t, new ColumnMap(requestedColumns, Const.ALL_COLUMNS.size()));
        }

        for (String requestedColumn: requestedColumns){
            readColumnValue(header, start, end, requestedColumn, results);
        }
        return Arrays.asList(results);
    }

    public static void readColumnValue(Header header, int start, int end, String requestedColumn, Row[] results) throws IOException {
        int count = end + 1;
        long[] positions = header.positions;
        int[] lengths = header.lengths;

        Column column = Const.COLUMNS_INDEX.get(requestedColumn);
        int index = column.getIndex();
        ColumnValue.ColumnType type = column.getType();

        ByteBuffer readBuffer = Context.getBlockReadBuffer();
        readBuffer.clear();
        column.getData().read(readBuffer, positions[index],  lengths[index]);
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
            Row result = results[i - start];
            switch (type){
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    result.getColumns().put(requestedColumn, new ColumnValue.DoubleFloatColumn(doubleValues[i]));
                    break;
                case COLUMN_TYPE_INTEGER:
                    result.getColumns().put(requestedColumn, new ColumnValue.IntegerColumn(intValues[i]));
                    break;
                case COLUMN_TYPE_STRING:
                    result.getColumns().put(requestedColumn, new ColumnValue.StringColumn(stringValues[i]));
                    break;
            }
        }
    }

    public static void aggregate(Header header, int start, int end, String requestedColumn, Aggregator aggregator) throws IOException {
        Column column = Const.COLUMNS_INDEX.get(requestedColumn);
        int index = column.getIndex();
        ColumnValue.ColumnType type = column.getType();

        int requestedCount = end - start + 1;
        boolean isDownsample = aggregator instanceof Downsample;
        if (!isDownsample && aggregator.getColumnFilter() == null && requestedCount == header.count){
            // return first
            switch (aggregator.getAggregator()){
                case MAX:
                    aggregator.accept(header.maxValues[index]);
                    break;
                case AVG:
                    aggregator.accept(header.sumValues[index], header.count);
                    break;
            }
            return;
        }

        int count = end + 1;
        long[] positions = header.positions;
        int[] lengths = header.lengths;

        ByteBuffer readBuffer = Context.getBlockReadBuffer();
        readBuffer.clear();
        column.getData().read(readBuffer, positions[index], lengths[index]);
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

        private final int count;

        private final long start;

        private final long[] positions;

        private final int[] lengths;

        private final double[] maxValues;

        private final double[] sumValues;

        public Header(int count, long start, long[] positions, int[] lengths, double[] maxValues, double[] sumValues) {
            this.count = count;
            this.start = start;
            this.positions = positions;
            this.lengths = lengths;
            this.maxValues = maxValues;
            this.sumValues = sumValues;
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

        public long[] getPositions() {
            return positions;
        }

        public int[] getLengths() {
            return lengths;
        }

        public double[] getMaxValues() {
            return maxValues;
        }

        public double[] getSumValues() {
            return sumValues;
        }
    }
}
