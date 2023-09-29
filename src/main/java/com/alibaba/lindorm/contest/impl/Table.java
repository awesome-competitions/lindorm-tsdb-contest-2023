package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Table {

    private final String name;

    private final String basePath;

    private Schema schema;

    private final Map<Integer, Index> indexes;

    private final List<String> sortedColumns;

    private final Map<String, Range> ranges;

    private final Data data;

    public Table(String basePath, String tableName) throws IOException {
        this.sortedColumns = new ArrayList<>(60);
        this.name = tableName;
        this.basePath = basePath;
        this.indexes = new ConcurrentHashMap<>(Const.MAX_VIN_COUNT, 0.65F);
        this.ranges = new ConcurrentHashMap<>();
        this.data = new Data(Path.of(basePath, tableName + ".data"), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
        this.schema.getColumnTypeMap().keySet().stream().sorted().forEach(sortedColumns::add);
    }

    public static Table load(String basePath, String tableName) throws IOException {
        Table t = new Table(basePath, tableName);
        t.loadSchema();
        t.loadDict();
        t.loadData();
        return t;
    }

    public String getName() {
        return name;
    }

    public Map<Integer, Index> getIndexes() {
        return indexes;
    }

    public Index getVinIndex(int vinId){
        return indexes.computeIfAbsent(vinId, k -> new Index());
    }

    public Range getRange(String column, ColumnValue.ColumnType type){
        return ranges.computeIfAbsent(column, k -> new Range(column, type));
    }

    public void upsert(Collection<Row> rows) throws IOException {
        for (Row row: rows){
            Index index = this.getVinIndex(row.getVin().getId());
            upsert(row, index);
        }
    }

    public void upsert(Row row, Index index) throws IOException {
        Context ctx = Context.get();
        BitBuffer writeBuffer = ctx.getWriteDataBuffer();
        writeBuffer.clear();
        writeBuffer.putInt(row.getVin().getId(), Const.VIN_ID_BITS);
        writeBuffer.putInt(Util.expressTimestamp(row.getTimestamp()), Const.TIMESTAMP_BITS);
        for(String col: sortedColumns){
            ColumnValue value = row.getColumns().get(col);
            ColumnValue.ColumnType type = value.getColumnType();
            int intVal = 0, intBits = 0;
            if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)){
                Range range = this.getRange(col, type);
                ByteBuffer stringValue = value.getStringValue();
                intVal = range.get(stringValue);
                intBits = Util.calculateBits(intVal, true);
            }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                double doubleVal = value.getDoubleFloatValue();
                intVal = (int) Math.round(doubleVal * Const.DOUBLE_EXPAND_MULTIPLE);
                intBits = Util.calculateBits(intVal, true);
            }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                intVal = value.getIntegerValue();
                intBits = Util.calculateBits(intVal, true);
            }
            writeBuffer.putInt(intBits, Const.INT_BYTES_BITS);
            writeBuffer.putInt(intVal, intBits);
        }
        writeBuffer.flip();
        BitBuffer tmpBuffer = ctx.getWriteTmpBuffer();
        tmpBuffer.clear();
        int len = writeBuffer.remaining();
        tmpBuffer.putShort((short) len);
        tmpBuffer.put(writeBuffer);
        tmpBuffer.flip();
        long position = this.data.write(tmpBuffer);
        index.put(row.getTimestamp(), Util.assembleLenAndPos(len, position), row);
    }

    public ArrayList<Row> executeLatestQuery(Collection<Vin> vins, Set<String> requestedColumns) throws IOException {
        ArrayList<Row> rows = new ArrayList<>();
        for(Vin vin: vins){
            Index index = this.getVinIndex(vin.getId());
            if(index == null || index.isEmpty()){
                continue;
            }
            Row lastRow = index.getLastestRow();
            if (lastRow != null){
                HashMap<String, ColumnValue> columnValues = new HashMap<>();
                for (String col: requestedColumns){
                    columnValues.put(col, lastRow.getColumns().get(col));
                }
                rows.add(new Row(lastRow.getVin(), lastRow.getTimestamp(), columnValues));
                continue;
            }
            long lastTimestamp = index.getLatestTimestamp();
            rows.add(new Row(vin, lastTimestamp, getColumnValues(index.get(lastTimestamp), requestedColumns)));
        }
        return rows;
    }

    public ArrayList<Row> executeTimeRangeQuery(Vin vin, long timeLowerBound, long timeUpperBound, Set<String> requestedColumns) {
        Index index = this.getVinIndex(vin.getId());
        if(index == null || index.isEmpty()){
            return Const.EMPTY_ROWS;
        }
        ArrayList<Row> rows = new ArrayList<>();
        index.forRangeEach(timeLowerBound, timeUpperBound, (timestamp, position) -> {
            try {
                rows.add(new Row(vin, timestamp, getColumnValues(position, requestedColumns)));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return rows;
    }

    public ArrayList<Row> executeAggregateQuery(Vin vin, long timeLowerBound, long timeUpperBound, String columnName, Aggregator aggregator) throws IOException {
        Index index = this.getVinIndex(vin.getId());
        if(index == null || index.isEmpty()){
            return Const.EMPTY_ROWS;
        }
        Set<String> requestedColumns = Collections.singleton(columnName);
        ColumnValue.ColumnType type = this.schema.getColumnTypeMap().get(columnName);
        List<Double> numbers = new ArrayList<>();
        index.forRangeEach(timeLowerBound, timeUpperBound, (timestamp, position) -> {
            try {
                Map<String, ColumnValue> columnValues = getColumnValues(position, requestedColumns);
                ColumnValue value = columnValues.get(columnName);
                if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)){
                    numbers.add((double) value.getIntegerValue());
                }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                    numbers.add(value.getDoubleFloatValue());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        if (numbers.size() == 0){
            return Const.EMPTY_ROWS;
        }

        Double d = null;
        if(aggregator.equals(Aggregator.AVG)){
            double sum = 0;
            for(double n: numbers){
                sum += n;
            }
            d = sum/numbers.size();
        }else if (aggregator.equals(Aggregator.MAX)){
            for(double n: numbers){
                if (d == null || d < n){
                    d = n;
                }
            }
        }
        Map<String, ColumnValue> columnValues = new HashMap<>();
        if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)){
            columnValues.put(columnName, new ColumnValue.IntegerColumn((int)d.doubleValue()));
        }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
            columnValues.put(columnName, new ColumnValue.DoubleFloatColumn(d));
        }
        ArrayList<Row> rows = new ArrayList<>();
        rows.add(new Row(vin, timeLowerBound, columnValues));
        return rows;
    }

    public ArrayList<Row> executeDownsampleQuery(Vin vin, long timeLowerBound, long timeUpperBound, String columnName, Aggregator aggregator, long interval, CompareExpression columnFilter) throws IOException {
        Index index = this.getVinIndex(vin.getId());
        if(index == null || index.isEmpty()){
            return Const.EMPTY_ROWS;
        }
        Set<String> requestedColumns = Collections.singleton(columnName);
        ColumnValue.ColumnType type = this.schema.getColumnTypeMap().get(columnName);
        List<Double> numbers = new ArrayList<>();
        index.forRangeEach(timeLowerBound, timeUpperBound, (timestamp, position) -> {
            try {
                Map<String, ColumnValue> columnValues = getColumnValues(position, requestedColumns);
                ColumnValue value = columnValues.get(columnName);
                if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)){
                    numbers.add((double) value.getIntegerValue());
                }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                    numbers.add(value.getDoubleFloatValue());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return null;
    }


    private Map<String, ColumnValue> getColumnValues(long position, Set<String> requestedColumns) throws IOException {
        Context ctx = Context.get();
        int len = Util.getLen(position);
        position = Util.getPosition(position);
        BitBuffer readBuffer = ctx.getReadDataBuffer();
        readBuffer.clear();
        this.data.read(readBuffer, position + Const.ROW_LEN_BYTES, len);
        readBuffer.flip();
        readBuffer.skip(Const.VIN_ID_BITS);
        readBuffer.skip(Const.TIMESTAMP_BITS);
        Map<String, ColumnValue> columnValue = new HashMap<>();
        for (String col : sortedColumns) {
            ColumnValue.ColumnType type = schema.getColumnTypeMap().get(col);
            if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)){
                Range range = this.getRange(col, type);
                int intBits = readBuffer.getInt(Const.INT_BYTES_BITS);
                int bytesId = readBuffer.getInt(intBits);
                ByteBuffer stringValue = range.getStringValue(bytesId);
                if (requestedColumns.isEmpty() || requestedColumns.contains(col)) {
                    columnValue.put(col, new ColumnValue.StringColumn(stringValue));
                }
            }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                int intBits = readBuffer.getInt(Const.INT_BYTES_BITS);
                int intVal = readBuffer.getInt(intBits);
                double doubleVal =((double)intVal)/Const.DOUBLE_EXPAND_MULTIPLE;
                if (requestedColumns.isEmpty() || requestedColumns.contains(col)) {
                    columnValue.put(col, new ColumnValue.DoubleFloatColumn(doubleVal));
                }
            }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                int intBits = readBuffer.getInt(Const.INT_BYTES_BITS);
                int intVal = readBuffer.getInt(intBits);
                if (requestedColumns.isEmpty() || requestedColumns.contains(col)) {
                    columnValue.put(col, new ColumnValue.IntegerColumn(intVal));
                }
            }
        }
        return columnValue;
    }

    public void force() throws IOException {
        this.data.force();
        this.flushSchema();
        this.flushDict();
    }

    public long size() throws IOException {
        return this.data.size();
    }

    public void close() throws IOException {
        this.data.close();
    }

    public void flushSchema() throws IOException {
        Path schemaPath = Path.of(basePath, name+ ".schema");
        StringBuilder builder = new StringBuilder();
        for(Map.Entry<String, ColumnValue.ColumnType> entry : schema.getColumnTypeMap().entrySet()){
            builder.append(entry.getKey())
                    .append(":")
                    .append(entry.getValue().name())
                    .append("\n");
        }
        Files.write(schemaPath, builder.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void loadSchema() throws IOException {
        byte[] bytes = Files.readAllBytes(Path.of(basePath, name + ".schema"));
        String[] lines = new String(bytes).split("\n");
        Map<String, ColumnValue.ColumnType> columnTypeMap = new HashMap<>();
        for(String line: lines){
            String[] kv = line.split(":");
            String columnName = kv[0];
            ColumnValue.ColumnType columnType = ColumnValue.ColumnType.valueOf(kv[1]);
            columnTypeMap.put(columnName, columnType);
        }
        this.setSchema(new Schema(columnTypeMap));
    }

    public void loadData() throws IOException {
        this.data.foreach((buffer, len, position) -> {
            int vinId = buffer.getInt(Const.VIN_ID_BITS);
            int timestamp = buffer.getInt(Const.TIMESTAMP_BITS);
            Index index = this.getVinIndex(vinId);
            index.put(Util.unExpressTimestamp(timestamp), Util.assembleLenAndPos(len, position));
        });
    }

    public void flushDict() throws IOException {
        Path dictPath = Path.of(basePath, name+ ".dict");
        StringBuilder builder = new StringBuilder();
        for(Range range : this.ranges.values()){
            builder.append(range.toString()).append("\n");
        }
        Files.write(dictPath, builder.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public void loadDict() throws IOException {
        byte[] bs = Files.readAllBytes(Path.of(basePath, name + ".dict"));
        String[] lines = new String(bs).split("\n");
        for(String line: lines){
            Range range = new Range(line);
            this.ranges.put(range.getColumn(), range);
        }
    }

}
