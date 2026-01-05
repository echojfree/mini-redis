package com.mini.redis.storage.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis Hash 数据结构实现
 *
 * 使用 ConcurrentHashMap 实现线程安全的哈希表
 * 支持字段级别的操作
 *
 * 面试知识点：
 * 1. ConcurrentHashMap 的使用
 * 2. 哈希表的实现
 * 3. 线程安全设计
 *
 * @author Mini Redis
 */
public class RedisHash {

    /**
     * 底层存储结构
     * 使用 ConcurrentHashMap 保证线程安全
     */
    private final ConcurrentHashMap<String, String> hash;

    /**
     * 构造函数
     */
    public RedisHash() {
        this.hash = new ConcurrentHashMap<>();
    }

    /**
     * 设置字段值
     *
     * @param field 字段名
     * @param value 字段值
     * @return 如果是新字段返回 1，如果是更新返回 0
     */
    public int hset(String field, String value) {
        String oldValue = hash.put(field, value);
        return oldValue == null ? 1 : 0;
    }

    /**
     * 批量设置字段值
     *
     * @param fieldValues 字段-值对
     */
    public void hmset(Map<String, String> fieldValues) {
        hash.putAll(fieldValues);
    }

    /**
     * 获取字段值
     *
     * @param field 字段名
     * @return 字段值，如果不存在返回 null
     */
    public String hget(String field) {
        return hash.get(field);
    }

    /**
     * 批量获取字段值
     *
     * @param fields 字段名数组
     * @return 字段值数组
     */
    public String[] hmget(String... fields) {
        String[] result = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            result[i] = hash.get(fields[i]);
        }
        return result;
    }

    /**
     * 删除字段
     *
     * @param fields 要删除的字段
     * @return 成功删除的字段数量
     */
    public int hdel(String... fields) {
        int deleted = 0;
        for (String field : fields) {
            if (hash.remove(field) != null) {
                deleted++;
            }
        }
        return deleted;
    }

    /**
     * 检查字段是否存在
     *
     * @param field 字段名
     * @return 如果存在返回 true
     */
    public boolean hexists(String field) {
        return hash.containsKey(field);
    }

    /**
     * 获取所有字段和值
     *
     * @return 所有字段-值对的副本
     */
    public Map<String, String> hgetall() {
        return new HashMap<>(hash);
    }

    /**
     * 获取所有字段名
     *
     * @return 字段名集合
     */
    public Set<String> hkeys() {
        return hash.keySet();
    }

    /**
     * 获取所有字段值
     *
     * @return 字段值集合
     */
    public String[] hvals() {
        return hash.values().toArray(new String[0]);
    }

    /**
     * 获取字段数量
     *
     * @return 字段数量
     */
    public int hlen() {
        return hash.size();
    }

    /**
     * 字段值增加指定整数
     *
     * @param field 字段名
     * @param increment 增量
     * @return 增加后的值
     * @throws NumberFormatException 如果字段值不是整数
     */
    public long hincrby(String field, long increment) {
        return hash.compute(field, (k, v) -> {
            long currentValue = 0;
            if (v != null) {
                try {
                    currentValue = Long.parseLong(v);
                } catch (NumberFormatException e) {
                    throw new NumberFormatException("hash value is not an integer");
                }
            }
            return String.valueOf(currentValue + increment);
        }).equals("") ? 0 : Long.parseLong(hash.get(field));
    }

    /**
     * 仅当字段不存在时设置值
     *
     * @param field 字段名
     * @param value 字段值
     * @return 设置成功返回 1，字段已存在返回 0
     */
    public int hsetnx(String field, String value) {
        String result = hash.putIfAbsent(field, value);
        return result == null ? 1 : 0;
    }

    /**
     * 获取字段值的长度
     *
     * @param field 字段名
     * @return 值的长度，字段不存在返回 0
     */
    public int hstrlen(String field) {
        String value = hash.get(field);
        return value != null ? value.length() : 0;
    }

    /**
     * 检查是否为空
     *
     * @return 如果为空返回 true
     */
    public boolean isEmpty() {
        return hash.isEmpty();
    }

    /**
     * 获取大小
     *
     * @return Hash 表大小
     */
    public int size() {
        return hash.size();
    }

    /**
     * 清空所有字段
     */
    public void clear() {
        hash.clear();
    }
}