package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Aggregator {

    protected double sum;

    protected Double max;

    protected int count;

    protected int filteredCount;

    protected final ColumnValue.ColumnType type;

    protected final com.alibaba.lindorm.contest.structs.Aggregator aggregator;

    protected final CompareExpression columnFilter;

    public Aggregator(ColumnValue.ColumnType type, com.alibaba.lindorm.contest.structs.Aggregator aggregator, CompareExpression columnFilter) {
        this.aggregator = aggregator;
        this.columnFilter = columnFilter;
        this.type = type;
    }

    public boolean doCompare(double value1) {
        ColumnValue.ColumnType type = columnFilter.getValue().getColumnType();
        double value;
        switch (type){
            case COLUMN_TYPE_INTEGER:
                value = columnFilter.getValue().getIntegerValue();
                break;
            case COLUMN_TYPE_DOUBLE_FLOAT:
                value = columnFilter.getValue().getDoubleFloatValue();
                break;
            default:
                throw new IllegalArgumentException("Unsupported column type for comparing: " + type);
        }
        switch (columnFilter.getCompareOp()) {
            case EQUAL:
                return value == value1;
            case GREATER:
                return value1 > value;
            default:
                throw new IllegalArgumentException("Unsupported compare op: " + columnFilter.getCompareOp());
        }
    }

    public void accept(double val){
        this.accept(-1, val);
    }

    public void accept(double val, int count){
        this.accept(-1, val, count);
    }

    public void accept(long timestamp, double val){
        this.accept(timestamp, val, 1);
    }

    public void accept(long timestamp, double val, int count) {
        if(columnFilter != null && ! doCompare(val)){
            filteredCount ++;
            return;
        }
        this.count += count;
        this.sum += val;

        if (max == null || max < val){
            max = val;
        }
    }

    public ColumnValue value(){
        switch (type){
            case COLUMN_TYPE_INTEGER:
                if (aggregator.equals(com.alibaba.lindorm.contest.structs.Aggregator.AVG)) {
                    return new ColumnValue.DoubleFloatColumn(count == 0 ? Double.NEGATIVE_INFINITY : sum / count);
                }
                return new ColumnValue.IntegerColumn(count == 0 ? (int) Double.NEGATIVE_INFINITY : max.intValue());
            case COLUMN_TYPE_DOUBLE_FLOAT:
                if (aggregator.equals(com.alibaba.lindorm.contest.structs.Aggregator.AVG)) {
                    return new ColumnValue.DoubleFloatColumn(count == 0 ? Double.NEGATIVE_INFINITY : sum / count);
                }
                return new ColumnValue.DoubleFloatColumn(count == 0 ? Double.NEGATIVE_INFINITY: max);
        }
        return null;
    }

    public CompareExpression getColumnFilter() {
        return columnFilter;
    }

    public int getCount() {
        return count;
    }

    public int getFilteredCount() {
        return filteredCount;
    }

    public com.alibaba.lindorm.contest.structs.Aggregator getAggregator() {
        return aggregator;
    }
}
