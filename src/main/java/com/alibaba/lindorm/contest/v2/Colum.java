package com.alibaba.lindorm.contest.v2;

import com.alibaba.lindorm.contest.structs.ColumnValue;

public class Colum {

    private final int index;

    private final ColumnValue.ColumnType type;

    public Colum(int index, ColumnValue.ColumnType type) {
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
