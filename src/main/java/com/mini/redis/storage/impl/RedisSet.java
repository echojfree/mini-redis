package com.mini.redis.storage.impl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis Set 数据结构实现
 *
 * 使用 ConcurrentHashMap.newKeySet() 实现线程安全的集合
 * 支持集合的基本操作和集合运算
 *
 * 面试知识点：
 * 1. Set 数据结构的实现
 * 2. 集合运算（交集、并集、差集）
 * 3. 线程安全的集合实现
 *
 * @author Mini Redis
 */
public class RedisSet {

    /**
     * 底层存储结构
     * 使用 ConcurrentHashMap 的 KeySet 实现线程安全的 Set
     */
    private final Set<String> set;

    /**
     * 构造函数
     */
    public RedisSet() {
        this.set = ConcurrentHashMap.newKeySet();
    }

    /**
     * 添加元素到集合
     *
     * @param members 要添加的元素
     * @return 成功添加的元素数量
     */
    public int sadd(String... members) {
        int added = 0;
        for (String member : members) {
            if (set.add(member)) {
                added++;
            }
        }
        return added;
    }

    /**
     * 从集合中移除元素
     *
     * @param members 要移除的元素
     * @return 成功移除的元素数量
     */
    public int srem(String... members) {
        int removed = 0;
        for (String member : members) {
            if (set.remove(member)) {
                removed++;
            }
        }
        return removed;
    }

    /**
     * 获取集合中的所有元素
     *
     * @return 所有元素的集合
     */
    public Set<String> smembers() {
        return new HashSet<>(set);
    }

    /**
     * 检查元素是否在集合中
     *
     * @param member 要检查的元素
     * @return 如果存在返回 true
     */
    public boolean sismember(String member) {
        return set.contains(member);
    }

    /**
     * 获取集合的元素数量
     *
     * @return 元素数量
     */
    public int scard() {
        return set.size();
    }

    /**
     * 随机返回一个或多个元素（不删除）
     *
     * @param count 要返回的元素数量
     * @return 随机元素集合
     */
    public Set<String> srandmember(int count) {
        if (count == 0 || set.isEmpty()) {
            return new HashSet<>();
        }

        List<String> list = new ArrayList<>(set);
        Collections.shuffle(list);

        int actualCount = Math.min(Math.abs(count), list.size());
        Set<String> result = new HashSet<>();

        for (int i = 0; i < actualCount; i++) {
            result.add(list.get(i));
        }

        return result;
    }

    /**
     * 随机移除并返回一个元素
     *
     * @return 被移除的元素，如果集合为空返回 null
     */
    public String spop() {
        if (set.isEmpty()) {
            return null;
        }

        // 随机选择一个元素
        List<String> list = new ArrayList<>(set);
        String element = list.get(new Random().nextInt(list.size()));
        set.remove(element);
        return element;
    }

    /**
     * 将元素从一个集合移动到另一个集合
     *
     * @param destination 目标集合
     * @param member 要移动的元素
     * @return 移动成功返回 1，元素不存在返回 0
     */
    public int smove(RedisSet destination, String member) {
        if (set.remove(member)) {
            destination.sadd(member);
            return 1;
        }
        return 0;
    }

    /**
     * 计算与另一个集合的交集
     *
     * @param other 另一个集合
     * @return 交集
     */
    public Set<String> sinter(RedisSet other) {
        Set<String> result = new HashSet<>(set);
        result.retainAll(other.set);
        return result;
    }

    /**
     * 计算与另一个集合的并集
     *
     * @param other 另一个集合
     * @return 并集
     */
    public Set<String> sunion(RedisSet other) {
        Set<String> result = new HashSet<>(set);
        result.addAll(other.set);
        return result;
    }

    /**
     * 计算与另一个集合的差集
     *
     * @param other 另一个集合
     * @return 差集（this - other）
     */
    public Set<String> sdiff(RedisSet other) {
        Set<String> result = new HashSet<>(set);
        result.removeAll(other.set);
        return result;
    }

    /**
     * 检查是否为空
     *
     * @return 如果为空返回 true
     */
    public boolean isEmpty() {
        return set.isEmpty();
    }

    /**
     * 获取大小
     *
     * @return 集合大小
     */
    public int size() {
        return set.size();
    }

    /**
     * 清空集合
     */
    public void clear() {
        set.clear();
    }
}