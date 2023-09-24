package com.alibaba.lindorm.contest.impl;

import com.alibaba.lindorm.contest.structs.ColumnValue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Range {

    private final String column;

    private final ColumnValue.ColumnType type;

    private final AtomicInteger id;

    private final Map<Object, Integer> kv;

    private final Map<Integer, Object> vk;

    public Range(String column, ColumnValue.ColumnType t){
        this.column = column;
        this.type = t;
        this.id = new AtomicInteger(0);
        this.kv = new ConcurrentHashMap<>(0);
        this.vk = new ConcurrentHashMap<>(0);
    }

    public Range(String str){
        String[] infos = str.split(":", 3);
        this.column = infos[0];
        this.type = ColumnValue.ColumnType.valueOf(infos[1]);
        this.id = new AtomicInteger(0);
        this.kv = new ConcurrentHashMap<>(0);
        this.vk = new ConcurrentHashMap<>(0);
        if (! infos[2].equals("")){
            String[] kvs = infos[2].split(";");
            for (String kvStr: kvs){
                String[] kv = kvStr.split(":", 2);
                Object o = null;
                if (this.type == ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
                    o = ByteBuffer.wrap(kv[1].getBytes());
                }else if (this.type == ColumnValue.ColumnType.COLUMN_TYPE_INTEGER) {
                    o = Integer.parseInt(kv[1]);
                }else if (this.type == ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT) {
                    o = Double.parseDouble(kv[1]);
                }
                this.get(o);
            }
        }
    }

    public int get(Object o){
        return kv.computeIfAbsent(o, k -> {
            int v = id.getAndIncrement();
            vk.put(v, o);
            return v;
        });
    }


    public Object getObject(int id){
        return vk.get(id);
    }

    public ByteBuffer getStringValue(int id){
       Object o = vk.get(id);
       if (o == null){
           return null;
       }
       return (ByteBuffer) o;
    }

    public int getIntegerValue(int id){
        Object o = vk.get(id);
        if (o == null){
            return 0;
        }
        return (int)o;
    }

    public double getDoubleFloatValue(int id){
        Object o = vk.get(id);
        if (o == null){
            return 0;
        }
        return (double)o;
    }

    public ColumnValue.ColumnType getType() {
        return type;
    }

    public String getColumn() {
        return column;
    }

    public String toString(){
        StringBuilder builder = new StringBuilder();
        builder.append(this.column).append(":").append(this.type).append(":");
        for(Map.Entry<Integer, Object> entry : this.vk.entrySet()){
            Object v = entry.getValue();
            if (this.type == ColumnValue.ColumnType.COLUMN_TYPE_STRING) {
                v = new String(((ByteBuffer) v).array());
            }
            builder.append(entry.getKey())
                    .append(":")
                    .append(v)
                    .append(";");
        }
        return builder.toString();
    }


}
