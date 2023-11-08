package com.alibaba.lindorm.contest.util;

import com.alibaba.lindorm.contest.structs.ColumnValue;
import com.alibaba.lindorm.contest.v2.Const;

import java.util.*;

public class ColumnMap extends AbstractMap<String, ColumnValue> implements Map<String, ColumnValue> {

    private final Collection<String> filter;

    private final ColumnValue[] values;

    public ColumnMap(Collection<String> filter, int size) {
        this.filter = filter;
        this.values = new ColumnValue[size];
    }

    @Override
    public Set<Entry<String, ColumnValue>> entrySet() {
        Set<Entry<String, ColumnValue>> entrySet = new HashSet<>();
        for (String columnName: filter){
            entrySet.add(new AbstractMap.SimpleEntry<>(columnName, values[Const.COLUMNS_INDEX.get(columnName).getIndex()]));
        }
        return entrySet;
    }

    @Override
    public int size() {
        return filter.size();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return filter.contains(key);
    }

    @Override
    public ColumnValue get(Object key) {
        if (!containsKey(key)){
            return null;
        }
        return values[Const.COLUMNS_INDEX.get(key).getIndex()];
    }

    @Override
    public ColumnValue put(String key, ColumnValue value) {
        values[Const.COLUMNS_INDEX.get(key).getIndex()] = value;
        return value;
    }

    @Override
    public ColumnValue remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends ColumnValue> m) {
        super.putAll(m);
    }

    @Override
    public void clear() {
        super.clear();
    }

    @Override
    public Set<String> keySet() {
        return new HashSet<>(filter);
    }

    @Override
    public Collection<ColumnValue> values() {
        List<ColumnValue> columnValues = new ArrayList<>();
        for (String columnName: filter){
            columnValues.add(values[Const.COLUMNS_INDEX.get(columnName).getIndex()]);
        }
        return columnValues;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
