package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.structs.CompareExpression;
import com.alibaba.lindorm.contest.structs.Row;

public class Downsample extends Aggregator {

    private final long timeLowerBound;

    private final long interval;

    private final Aggregator[] aggregators;

    public Downsample(long timeLowerBound, long timeUpperBound, long interval, ColumnValue.ColumnType type, com.alibaba.lindorm.contest.structs.Aggregator aggregator, CompareExpression columnFilter) {
        super(type, aggregator, columnFilter);
        this.timeLowerBound = timeLowerBound;
        this.interval = interval;
        this.aggregators = new Aggregator[(int) ((timeUpperBound - timeLowerBound)/interval)];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = new Aggregator(type, aggregator, columnFilter);
        }
    }
    public void accept(long timestamp, double val, int count) {
        int index = (int) ((timestamp - timeLowerBound)/ (int) interval);
        aggregators[index].accept(timestamp, val, count);
    }

    public Aggregator[] getAggregators() {
        return aggregators;
    }
}
