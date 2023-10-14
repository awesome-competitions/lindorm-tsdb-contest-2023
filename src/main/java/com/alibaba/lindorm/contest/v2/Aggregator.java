package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Aggregator implements BiConsumer<Double, Integer> {

    private double sum;

    private Double max;

    private int count;

    private int filteredCount;

    private final ColumnValue.ColumnType type;

    private final com.alibaba.lindorm.contest.structs.Aggregator aggregator;

    private final CompareExpression columnFilter;

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

    public void accept(Double val){
        this.accept(val, 1);
    }

    @Override
    public void accept(Double val, Integer count) {
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
