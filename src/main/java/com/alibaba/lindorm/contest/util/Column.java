package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.structs.ColumnValue;

public class Column {

    private final int index;

    private final ColumnValue.ColumnType type;

    public Column(int index, ColumnValue.ColumnType type) {
        this.index = index;
        this.type = type;
    }

    public int getIndex() {
        return index;
    }

    public ColumnValue.ColumnType getType() {
        return type;
    }
}
