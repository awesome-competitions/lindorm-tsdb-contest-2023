package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    private final Data[] data;

    public Table(String basePath, String tableName) throws IOException {
        this.name = tableName;
        this.basePath = basePath;
        this.indexes = new ConcurrentHashMap<>(Const.VIN_COUNT, 0.65F);
        this.vinIds = new ConcurrentHashMap<>();
        this.data = new Data[Const.DATA_FILE_COUNT];
        for (int i = 0; i < Const.DATA_FILE_COUNT; i++){
            this.data[i] = new Data(Path.of(basePath, tableName + ".data." + i), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
        }
    }

    public void setSchema(Schema schema) {
        Const.SORTED_COLUMNS.clear();
        Const.COLUMNS_INDEX.clear();

        this.schema = schema;
        this.schema.getColumnTypeMap().keySet().stream().sorted().forEach(Const.SORTED_COLUMNS::add);
        for (int i = 0; i < Const.SORTED_COLUMNS.size(); i++) {
            String columnName = Const.SORTED_COLUMNS.get(i);
            Const.COLUMNS_INDEX.put(Const.SORTED_COLUMNS.get(i), new Colum(i, schema.getColumnTypeMap().get(columnName)));
        }
    }

    public static Table load(String basePath, String tableName) throws IOException {
        Table t = new Table(basePath, tableName);
        t.loadSchema();
        t.loadIndex();
        return t;
    }

    public String getName() {
        return name;
    }

    public Map<Integer, Index> getIndexes() {
        return indexes;
    }

    public Index getOrCreateVinIndex(Integer vinId){
        return indexes.computeIfAbsent(vinId, k -> new Index(this.data[vinId%Const.DATA_FILE_COUNT], vinId));
    }

    public Index getOrCreateVinIndex(Vin vin){
        return getOrCreateVinIndex(vinIds.computeIfAbsent(vin, Util::parseVinId));
    }

    public Index getVinIndex(Vin vin){
        return indexes.get(vinIds.computeIfAbsent(vin, Util::parseVinId));
    }

    public void upsert(Collection<Row> rows) throws IOException {
        for (Row row: rows){
            Index index = this.getOrCreateVinIndex(row.getVin());
            index.insert(row.getTimestamp(), row.getColumns());
        }
    }

    public ArrayList<Row> executeLatestQuery(Collection<Vin> vins, Set<String> requestedColumns) throws IOException {
        ArrayList<Row> rows = new ArrayList<>();
        for(Vin vin: vins){
            Index index = this.getVinIndex(vin);
            if(index == null){
                continue;
            }
            Map<String, ColumnValue> columnValue = index.getLatest(requestedColumns);
            if (columnValue == null){
                continue;
            }
            rows.add(new Row(vin, index.getLatestTimestamp(), columnValue));
        }
        return rows;
    }

    public ArrayList<Row> executeTimeRangeQuery(Vin vin, long timeLowerBound, long timeUpperBound, Set<String> requestedColumns) throws IOException {
        Index index = this.getVinIndex(vin);
        if(index == null){
            return Const.EMPTY_ROWS;
        }
        ArrayList<Row> rows = new ArrayList<>();
        Map<Long, Map<String, ColumnValue>> results = index.range(timeLowerBound, timeUpperBound, requestedColumns);
        results.forEach((timestamp, columnValues) -> rows.add(new Row(vin, timestamp, columnValues)));
        return rows;
    }

    public ArrayList<Row> executeAggregateQuery(Vin vin, long timeLowerBound, long timeUpperBound, String columnName, Aggregator aggregator) throws IOException {
        Index index = this.getVinIndex(vin);
        if(index == null){
            return Const.EMPTY_ROWS;
        }

        ColumnValue.ColumnType type = this.schema.getColumnTypeMap().get(columnName);
        Map<Long, ColumnValue> results = index.range(timeLowerBound, timeUpperBound, columnName);
        if (results.size() == 0){
            return Const.EMPTY_ROWS;
        }

        ArrayList<Row> rows = new ArrayList<>();
        rows.add(handleAggregate(vin, timeLowerBound, results.values(), type, columnName, aggregator, null));
        return rows;
    }

    public ArrayList<Row> executeDownsampleQuery(Vin vin, long timeLowerBound, long timeUpperBound, String columnName, Aggregator aggregator, long interval, CompareExpression columnFilter) throws IOException {
        Index index = this.getVinIndex(vin);
        if(index == null){
            return Const.EMPTY_ROWS;
        }
        ColumnValue.ColumnType type = this.schema.getColumnTypeMap().get(columnName);
        Map<Long, ColumnValue> results = index.range(timeLowerBound, timeUpperBound, columnName);
        if (results.size() == 0){
            return Const.EMPTY_ROWS;
        }

        int size = (int) ((timeUpperBound - timeLowerBound)/interval);
        List<List<ColumnValue>> group = new ArrayList<>(size);
        for (int i= 0; i < size; i++){
            group.add(new ArrayList<>());
        }

        results.forEach((timestamp, value) -> {
            int groupIndex = (int) ((timestamp - timeLowerBound)/interval);
            List<ColumnValue> columnValues = group.get(groupIndex);
            columnValues.add(value);
        });

        ArrayList<Row> rows = new ArrayList<>();
        long subTimeLowerBound = timeLowerBound;
        for(List<ColumnValue> numbers: group){
            Row row = handleAggregate(vin, subTimeLowerBound, numbers, type, columnName, aggregator, columnFilter);
            if (row != null){
                rows.add(row);
            }
            subTimeLowerBound += interval;
        }
        return rows;
    }

    private Row handleAggregate(Vin vin, long timeLowerBound, Collection<ColumnValue> columnValues, ColumnValue.ColumnType type, String columnName, Aggregator aggregator, CompareExpression columnFilter) {
        if (columnValues == null || columnValues.size() == 0){
            return null;
        }

        Double d = null;
        if(aggregator.equals(Aggregator.AVG)){
            double sum = 0;
            int count = 0;
            for(ColumnValue v: columnValues){
                if(columnFilter != null && !columnFilter.doCompare(v)){
                    continue;
                }
                count ++;
                if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)){
                    sum += v.getIntegerValue();
                }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)){
                    sum += v.getDoubleFloatValue();
                }
            }
            if(count > 0){
                d = sum/count;
            }
        }else if (aggregator.equals(Aggregator.MAX)){
            for(ColumnValue v: columnValues){
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
        Map<String, ColumnValue> result = new HashMap<>();
        if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_INTEGER)){
            if(aggregator.equals(Aggregator.AVG)){
                result.put(columnName, new ColumnValue.DoubleFloatColumn(d));
            }else{
                result.put(columnName, new ColumnValue.IntegerColumn((int)d.doubleValue()));
            }
        }else if (type.equals(ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT)) {
            result.put(columnName, new ColumnValue.DoubleFloatColumn(d));
        }
        return new Row(vin, timeLowerBound, result);
    }

    public void force() throws IOException {
        for (Index index: indexes.values()){
            index.flush();
        }
        for (Data f: data){
            f.force();
        }
        this.flushSchema();
        this.flushIndex();
    }

    public long size() throws IOException {
        long size = 0;
        for (Data f: data){
            size += f.size();
        }
        return size;
    }

    public void close() throws IOException {
        for (Data f: data){
            f.close();
        }
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

    public void flushIndex() throws IOException {
        Path indexPath = Path.of(basePath, name+ ".index");
        FileChannel ch = FileChannel.open(indexPath, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);

        // single index
        Index singleIndex = null;
        for (Index index: indexes.values()){
            singleIndex = index;
            break;
        }
        if(singleIndex == null){
            return;
        }
        long oldest = singleIndex.getOldestTimestamp();

        // write first timestamp
        ByteBuffer buffer = ByteBuffer.allocateDirect(4 * Const.M);
        buffer.putLong(oldest);
        for (Index index: indexes.values()){
            buffer.putInt(index.getVin());
            int size = singleIndex.size();
            buffer.putInt(size);
            for (long i = oldest; i < oldest + size * 1000L; i += 1000){
                buffer.putLong(index.get(i));
            }
            buffer.flip();
            ch.write(buffer);
            buffer.clear();
        }
    }

    public void loadIndex() throws IOException {
        Path indexPath = Path.of(basePath, name+ ".index");
        FileChannel ch = FileChannel.open(indexPath, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);

        ByteBuffer buffer = ByteBuffer.allocateDirect(4 * Const.M);
        buffer.limit(8);
        ch.read(buffer);
        buffer.flip();
        long oldest = buffer.getLong();
        while (true){
            buffer.clear();
            buffer.limit(4 + 4);
            if (ch.read(buffer) != buffer.limit()){
                break;
            }
            buffer.flip();
            int vinId = buffer.getInt();
            int size = buffer.getInt();

            buffer.clear();
            buffer.limit(size * 8);
            if (ch.read(buffer) != buffer.limit()){
                break;
            }
            buffer.flip();

            Index index = getOrCreateVinIndex(vinId);
            for (long i = oldest; i < oldest + size * 1000L; i += 1000){
                long pos = buffer.getLong();
                if (pos != -1){
                    index.insert(i, pos);
                }
            }
            // load latest
            index.getLatest(Const.EMPTY_COLUMNS);
        }
    }

}
