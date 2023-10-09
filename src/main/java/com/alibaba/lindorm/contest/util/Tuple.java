package com.alibaba.lindorm.contest.util;

public class Tuple<K,V> {

    private K k;

    private V v;

    public Tuple(K k, V v){
        this.k = k;
        this.v = v;
    }

    public K K() {
        return k;
    }

    public V V() {
        return v;
    }
}
