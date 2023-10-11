package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.Aggregator;
import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;

import java.util.function.Consumer;

public class AggregateConsumer implements Consumer<Double> {

    private double sum;

    private Double max;

    private int count;

    private int filteredCount;

    private final ColumnValue.ColumnType type;

    private final Aggregator aggregator;

    private final CompareExpression columnFilter;

    public AggregateConsumer(ColumnValue.ColumnType type, Aggregator aggregator, CompareExpression columnFilter) {
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

    @Override
    public void accept(Double d) {
        if(columnFilter != null && ! doCompare(d)){
            filteredCount ++;
            return;
        }
        this.count ++;
        this.sum += d;

        if (max == null || max < d){
            max = d;
        }
    }

    public ColumnValue value(){
        switch (type){
            case COLUMN_TYPE_INTEGER:
                if (count == 0){
                    return new ColumnValue.IntegerColumn((int) Double.NEGATIVE_INFINITY);
                }
                if (aggregator.equals(Aggregator.AVG)) {
                    return new ColumnValue.DoubleFloatColumn(sum / count);
                }
                return new ColumnValue.IntegerColumn(max.intValue());
            case COLUMN_TYPE_DOUBLE_FLOAT:
                if (count == 0){
                    return new ColumnValue.DoubleFloatColumn(Double.NEGATIVE_INFINITY);
                }
                if (aggregator.equals(Aggregator.AVG)) {
                    return new ColumnValue.DoubleFloatColumn(sum / count);
                }
                return new ColumnValue.DoubleFloatColumn(max);
        }
        return null;
    }

    public int getCount() {
        return count;
    }

    public int getFilteredCount() {
        return filteredCount;
    }
}
