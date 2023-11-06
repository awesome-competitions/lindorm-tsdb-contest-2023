package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.v2.Data;

public class Column {

    private final int index;

    private final ColumnValue.ColumnType type;

    private final Data data;

    public Column(int index, ColumnValue.ColumnType type, Data data) {
        this.index = index;
        this.type = type;
        this.data = data;
    }

    public int getIndex() {
        return index;
    }

    public ColumnValue.ColumnType getType() {
        return type;
    }

    public Data getData() {
        return data;
    }
}
