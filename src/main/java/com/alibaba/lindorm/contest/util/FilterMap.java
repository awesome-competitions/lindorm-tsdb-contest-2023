package com.alibaba.lindorm.contest.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Filter;
import java.util.stream.Collectors;

public class FilterMap<K, V> extends HashMap<K, V> implements Map<K, V> {

    private final Map<K, V> source;

    private final Set<K> filter;

    public FilterMap(Map<K, V> source, Set<K> filter) {
        this.source = source;
        this.filter = filter;
    }

    @Override
    public V get(Object key) {
        if (filter.isEmpty() || filter.contains(key)){
            return source.get(key);
        }
        return null;
    }

    @Override
    public int size(){
        if (filter.isEmpty()){
            return source.size();
        }
        return filter.size();
    }

    @Override
    public boolean containsKey(Object key){
        if (filter.isEmpty()){
            return source.containsKey(key);
        }
        return filter.contains(key);
    }

    @Override
    public Set<K> keySet(){
        if (filter.isEmpty()){
            return source.keySet();
        }
        return filter;
    }

    @Override
    public Collection<V> values(){
        if (filter.isEmpty()){
            return source.values();
        }
        return source.entrySet().stream().filter(e -> filter.contains(e.getKey())).map(Map.Entry::getValue).collect(Collectors.toSet());
    }

    @Override
    public Set<Entry<K, V>> entrySet(){
        if (filter.isEmpty()){
            return source.entrySet();
        }
        return source.entrySet().stream().filter(e -> filter.contains(e.getKey())).collect(Collectors.toSet());
    }



}
