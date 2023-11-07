package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.util.File;
import com.alibaba.lindorm.contest.util.Tuple;
import com.alibaba.lindorm.contest.util.Util;
import com.alibaba.lindorm.contest.util.Column;

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

    private final Map<Vin, Index> indexes;

    public Table(String basePath, String tableName) {
        this.name = tableName;
        this.basePath = basePath;
        this.indexes = new ConcurrentHashMap<>(Const.VIN_COUNT, 0.65F);
    }

    public void setSchema(Schema schema) throws IOException {
        Const.ALL_COLUMNS.clear();
        Const.INT_COLUMNS.clear();
        Const.DOUBLE_COLUMNS.clear();
        Const.STRING_COLUMNS.clear();
        Const.COLUMNS_INDEX.clear();
        this.schema = schema;

        List<String> columnNames = new ArrayList<>(schema.getColumnTypeMap().keySet());
        columnNames.sort(Comparator.naturalOrder());
        for (String columnName: columnNames){
            ColumnValue.ColumnType columnType = schema.getColumnTypeMap().get(columnName);
            Data file = new DiskData(Path.of(basePath, this.name + "_" + columnName + ".data"),
                    StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
            switch (columnType){
                case COLUMN_TYPE_INTEGER:
                    Const.COLUMNS_INDEX.put(columnName, new Column(Const.INT_COLUMNS.size(), columnType, file));
                    Const.INT_COLUMNS.add(columnName);
                    break;
                case COLUMN_TYPE_DOUBLE_FLOAT:
                    Const.COLUMNS_INDEX.put(columnName, new Column(Const.DOUBLE_COLUMNS.size(), columnType, file));
                    Const.DOUBLE_COLUMNS.add(columnName);
                    break;
                case COLUMN_TYPE_STRING:
                    Const.COLUMNS_INDEX.put(columnName, new Column(Const.STRING_COLUMNS.size(), columnType, file));
                    Const.STRING_COLUMNS.add(columnName);
                    break;
            }
        }
        Const.ALL_COLUMNS.addAll(Const.INT_COLUMNS);
        Const.ALL_COLUMNS.addAll(Const.DOUBLE_COLUMNS);
        Const.ALL_COLUMNS.addAll(Const.STRING_COLUMNS);
    }

    public static Table load(String basePath, String tableName) throws IOException {
        Table t = new Table(basePath, tableName);
        t.loadSchema();
        t.loadData();
        t.loadIndex();
        return t;
    }

    public String getName() {
        return name;
    }

    public Map<Vin, Index> getIndexes() {
        return indexes;
    }

    public Index getOrCreateVinIndex(Vin vin){
        return indexes.computeIfAbsent(vin, k -> new Index(vin));
    }

    public Index getVinIndex(Vin vin){
        return indexes.get(vin);
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
        return index.range(timeLowerBound, timeUpperBound, requestedColumns);
    }

    public ArrayList<Row> executeAggregateQuery(Vin vin, long timeLowerBound, long timeUpperBound, String columnName, com.alibaba.lindorm.contest.structs.Aggregator aggregator) throws IOException {
        Index index = this.getVinIndex(vin);
        if(index == null){
            return Const.EMPTY_ROWS;
        }

        ColumnValue.ColumnType type = this.schema.getColumnTypeMap().get(columnName);
        Aggregator agg = new Aggregator(type, aggregator, null);
        index.aggregate(timeLowerBound, timeUpperBound, columnName, agg);
        if (agg.getCount() == 0){
            return Const.EMPTY_ROWS;
        }

        ArrayList<Row> rows = new ArrayList<>();
        rows.add(new Row(vin, timeLowerBound, Map.of(columnName, agg.value())));
        return rows;
    }

    public ArrayList<Row> executeDownsampleQuery(Vin vin, long timeLowerBound, long timeUpperBound, String columnName, com.alibaba.lindorm.contest.structs.Aggregator aggregator, long interval, CompareExpression columnFilter) throws IOException {
        Index index = this.getVinIndex(vin);
        if(index == null){
            return Const.EMPTY_ROWS;
        }
        ArrayList<Row> rows = new ArrayList<>();
        ColumnValue.ColumnType type = this.schema.getColumnTypeMap().get(columnName);

        Downsample downsample = new Downsample(timeLowerBound, timeUpperBound, interval, type, aggregator, columnFilter);
        index.aggregate(timeLowerBound, timeUpperBound, columnName, downsample);
        Aggregator[] aggregators = downsample.getAggregators();
        for (long start = timeLowerBound; start < timeUpperBound; start += interval){
            int i = (int) ((start - timeLowerBound) / interval);
            Aggregator agg = aggregators[i];
            if (agg.getCount() == 0 && agg.getFilteredCount() == 0){
                continue;
            }
            rows.add(new Row(vin, start, Map.of(columnName, agg.value())));
        }
        return rows;
    }

    public void forceAndClose() throws IOException {
        this.flushSchema();
        this.flushData();
        this.flushIndex();
    }

    public long size() throws IOException {
        long size = 0;
        for (Column column: Const.COLUMNS_INDEX.values()){
            size += column.getData().size();
        }
        return size;
    }

    public void flushData() throws IOException {
        for (Index index: indexes.values()){
            index.flush();
        }
        for (Map.Entry<String, Column> e: Const.COLUMNS_INDEX.entrySet()){
            String columnName = e.getKey();
            Data data = e.getValue().getData();
            if (data instanceof MemData){
                continue;
            }
            data.force();
            data.close();
            if (Const.COMPRESS_COLUMNS.contains(columnName)){
                File.compress(data.path());
            }
        }
    }

    public void loadData() throws IOException {
        for (Map.Entry<String, Column> e: Const.COLUMNS_INDEX.entrySet()){
            String columnName = e.getKey();
            Data data = e.getValue().getData();
            if (Const.COMPRESS_COLUMNS.contains(columnName)){
                ByteBuffer buff = File.decompress(data.path());
                e.getValue().setData(new MemData(buff));
            }
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

        int intCount = Const.INT_COLUMNS.size();
        int doubleCount = Const.DOUBLE_COLUMNS.size();
        int numberCount = intCount + doubleCount;
        int stringCount = Const.STRING_COLUMNS.size();
        int columnCount = numberCount + stringCount;

        // write first timestamp
        ByteBuffer buffer = ByteBuffer.allocate(4 * Const.M);
        for (Index index: indexes.values()){
            buffer.clear();
            List<Block.Header> headers = index.getHeaders();
            buffer.put(index.getVin().getVin());
            buffer.putShort((short) headers.size());
            for (Block.Header header: headers){
                buffer.putShort((short) header.getCount());
                buffer.putLong(header.getStart());
                for (int i = 0; i < numberCount; i ++){
                    buffer.putInt((int) header.getPositions()[i]);
                }
                for (int i = numberCount; i < columnCount; i ++){
                    buffer.putLong(header.getPositions()[i]);
                }
                for (int i = 0; i < intCount; i ++){
                    buffer.putInt((int) header.getMaxValues()[i]);
                    buffer.putDouble(header.getSumValues()[i]);
                }
                for (int i = intCount; i < numberCount; i ++){
                    buffer.putDouble(header.getMaxValues()[i]);
                    buffer.putDouble(header.getSumValues()[i]);
                }
            }
            buffer.flip();
            ch.write(buffer);
        }
        ch.force(true);
        ch.close();
        File.compress(indexPath);
        System.out.println("index file size:" + File.getFileSize(indexPath));
    }

    public void loadIndex() throws IOException {
        Path indexPath = Path.of(basePath, name+ ".index");

        int intCount = Const.INT_COLUMNS.size();
        int doubleCount = Const.DOUBLE_COLUMNS.size();
        int numberCount = intCount + doubleCount;
        int stringCount = Const.STRING_COLUMNS.size();
        int columnCount = numberCount + stringCount;
        ByteBuffer buffer = File.decompress(indexPath);
        List<Block.Header> headers = new ArrayList<>();
        while (buffer.remaining() > 0){
            Vin vin = new Vin(new byte[Const.VIN_LENGTH]);
            buffer.get(vin.getVin());
            int blockSize = buffer.getShort();
            Index index = getOrCreateVinIndex(vin);
            for (int i = 0; i < blockSize; i ++){
                int headerCount = buffer.getShort();
                long headerStart = buffer.getLong();
                long[] headerPositions = new long[columnCount];
                int[] headerLengths = new int[columnCount];
                double[] maxValues = new double[numberCount];
                double[] sumValues = new double[numberCount];
                for (int j = 0; j < numberCount; j ++){
                    headerPositions[j] = buffer.getInt();
                }
                for (int j = numberCount; j < columnCount; j ++){
                    headerPositions[j] = buffer.getLong();
                }
                for (int j = 0; j < intCount; j ++){
                    maxValues[j] = buffer.getInt();
                    sumValues[j] = buffer.getDouble();
                }
                for (int j = intCount; j < numberCount; j ++){
                    maxValues[j] = buffer.getDouble();
                    sumValues[j] = buffer.getDouble();
                }
                Block.Header header = new Block.Header(headerCount, headerStart, headerPositions, headerLengths, maxValues, sumValues);
                index.mark(header.getEnd());
                index.getHeaders().add(header);
                headers.add(header);
            }
        }

        for (int i = 0; i < columnCount; i ++){
            int index = i;
            // sort by position asc
            headers.sort(Comparator.comparingLong(o -> o.getPositions()[index]));
            // calculate length
            for (int j = 0; j < headers.size() - 1; j ++){
                Block.Header header = headers.get(j);
                Block.Header nextHeader = headers.get(j + 1);
                header.getLengths()[index] = (int) (nextHeader.getPositions()[index] - header.getPositions()[index]);
            }
            // last one
            String columnName = Const.ALL_COLUMNS.get(i);
            Column column = Const.COLUMNS_INDEX.get(columnName);
            Block.Header lastHeader = headers.get(headers.size() - 1);
            lastHeader.getLengths()[index] = (int) (column.getData().size() - lastHeader.getPositions()[index]);
        }

        // load latest
        for (Index index: indexes.values()){
            index.getLatest(Const.EMPTY_COLUMNS);
        }
    }
}
