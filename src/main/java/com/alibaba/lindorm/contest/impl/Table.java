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

    private final Map<Vin, Integer> vinIds;

    private final List<String> sortedColumns;

    private final Map<String, Range> ranges;

    private final Data data;

    public Table(String basePath, String tableName) throws IOException {
        this.sortedColumns = new ArrayList<>(60);
        this.name = tableName;
        this.basePath = basePath;
        this.indexes = new ConcurrentHashMap<>(Const.MAX_VIN_COUNT, 0.65F);
        this.vinIds = new ConcurrentHashMap<>();
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

    public int getVinId(Vin vin){
        return vinIds.computeIfAbsent(vin, k -> Util.parseVinId(k.getVin()));
    }

    public Range getRange(String column, ColumnValue.ColumnType type){
        return ranges.computeIfAbsent(column, k -> new Range(column, type));
    }

    public void upsert(Collection<Row> rows) throws IOException {
        for (Row row: rows){
            Index index = this.getVinIndex(getVinId(row.getVin()));
            upsert(row, index);
        }
    }

    public void upsert(Row row, Index index) throws IOException {
        Context ctx = Context.get();
        BitBuffer writeBuffer = ctx.getWriteDataBuffer();
        writeBuffer.clear();
        writeBuffer.putInt(getVinId(row.getVin()), Const.VIN_ID_BITS);
        writeBuffer.putInt(Util.expressTimestamp(row.getTimestamp()), Const.TIMESTAMP_BITS);
        for(String col: sortedColumns){
            ColumnValue value = row.getColumns().get(col);
            ColumnValue.ColumnType type = value.getColumnType();
            int intVal = 0, intBits = 0;
            int symbol = 0;
            if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_STRING)){
                Range range = this.getRange(col, type);
                ByteBuffer stringValue = value.getStringValue();
                intVal = range.get(stringValue);
            }else {
                if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
                    double doubleVal = value.getDoubleFloatValue();
                    intVal = (int) Math.round(doubleVal * Const.DOUBLE_EXPAND_MULTIPLE);
                }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                    intVal = value.getIntegerValue();
                }
                if (intVal < 0){
                    symbol = 1;
                    intVal = -intVal;
                }
                writeBuffer.putInt(symbol, 1);
            }
            intBits = Util.calculateBits(intVal, true);
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
            Index index = this.getVinIndex(getVinId(vin));
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
        Index index = this.getVinIndex(getVinId(vin));
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
        Index index = this.getVinIndex(getVinId(vin));
        if(index == null || index.isEmpty()){
            return Const.EMPTY_ROWS;
        }
        Set<String> requestedColumns = Collections.singleton(columnName);
        ColumnValue.ColumnType type = this.schema.getColumnTypeMap().get(columnName);
        List<ColumnValue> numbers = new ArrayList<>();
        index.forRangeEach(timeLowerBound, timeUpperBound, (timestamp, position) -> {
            try {
                Map<String, ColumnValue> columnValues = getColumnValues(position, requestedColumns);
                ColumnValue value = columnValues.get(columnName);
                numbers.add(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        if (numbers.size() == 0){
            return Const.EMPTY_ROWS;
        }
        ArrayList<Row> rows = new ArrayList<>();
        rows.add(handleNumbers(vin, timeLowerBound, numbers, type, columnName, aggregator, null));
        return rows;
    }

    public ArrayList<Row> executeDownsampleQuery(Vin vin, long timeLowerBound, long timeUpperBound, String columnName, Aggregator aggregator, long interval, CompareExpression columnFilter) throws IOException {
        Index index = this.getVinIndex(getVinId(vin));
        if(index == null || index.isEmpty()){
            return Const.EMPTY_ROWS;
        }
        Set<String> requestedColumns = Collections.singleton(columnName);
        ColumnValue.ColumnType type = this.schema.getColumnTypeMap().get(columnName);

        int size = (int) ((timeUpperBound - timeLowerBound)/interval);
        List<ColumnValue>[] group = new List[size];
        for (int i= 0; i < group.length; i++){
            group[i] = new ArrayList<>();
        }
        index.forRangeEach(timeLowerBound, timeUpperBound, (timestamp, position) -> {
            try {
                Map<String, ColumnValue> columnValues = getColumnValues(position, requestedColumns);
                ColumnValue value = columnValues.get(columnName);
                int groupIndex = (int) ((timestamp - timeLowerBound)/interval);
                List<ColumnValue> numbers = group[groupIndex];
                numbers.add(value);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ArrayList<Row> rows = new ArrayList<>();
        long subTimeLowerBound = timeLowerBound;
        for(List<ColumnValue> numbers: group){
            Row row = handleNumbers(vin, subTimeLowerBound, numbers, type, columnName, aggregator, columnFilter);
            if (row != null){
                rows.add(row);
            }
            subTimeLowerBound += interval;
        }
        return rows;
    }

    private Row handleNumbers(Vin vin, long timeLowerBound, List<ColumnValue> numbers, ColumnValue.ColumnType type, String columnName, Aggregator aggregator, CompareExpression columnFilter) {
        if (numbers == null || numbers.size() == 0){
            return null;
        }

        Double d = null;
        if(aggregator.equals(Aggregator.AVG)){
            double sum = 0;
            boolean suc = false;
            for(ColumnValue v: numbers){
                if(columnFilter != null && !columnFilter.doCompare(v)){
                    continue;
                }
                suc = true;
                if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)){
                    sum += v.getIntegerValue();
                }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)){
                    sum += v.getDoubleFloatValue();
                }
            }
            if(suc){
                d = sum/numbers.size();
            }
        }else if (aggregator.equals(Aggregator.MAX)){
            for(ColumnValue v: numbers){
                if(columnFilter != null && !columnFilter.doCompare(v)){
                    continue;
                }
                double t = 0;
                if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)){
                    t = v.getIntegerValue();
                }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)){
                    t = v.getDoubleFloatValue();
                }
                if (d == null || d < t){
                    d = t;
                }
            }
        }
        if (d == null){
            d = Double.NEGATIVE_INFINITY;
        }
        Map<String, ColumnValue> columnValues = new HashMap<>();
        if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)){
            if(aggregator.equals(Aggregator.AVG)){
                columnValues.put(columnName, new ColumnValue.DoubleFloatColumn(d));
            }else{
                columnValues.put(columnName, new ColumnValue.IntegerColumn((int)d.doubleValue()));
            }
        }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
            columnValues.put(columnName, new ColumnValue.DoubleFloatColumn(d));
        }
        return new Row(vin, timeLowerBound, columnValues);
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
                int symbol = readBuffer.getInt(1);
                int intBits = readBuffer.getInt(Const.INT_BYTES_BITS);
                int intVal = readBuffer.getInt(intBits);
                double doubleVal =((double)intVal)/Const.DOUBLE_EXPAND_MULTIPLE;
                if (symbol == 1){
                    doubleVal = -doubleVal;
                }
                if (requestedColumns.isEmpty() || requestedColumns.contains(col)) {
                    columnValue.put(col, new ColumnValue.DoubleFloatColumn(doubleVal));
                }
            }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)) {
                int symbol = readBuffer.getInt(1);
                int intBits = readBuffer.getInt(Const.INT_BYTES_BITS);
                int intVal = readBuffer.getInt(intBits);
                if (symbol == 1){
                    intVal = -intVal;
                }
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
