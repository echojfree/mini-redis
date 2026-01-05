package com.mini.redis.storage.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Redis List 数据结构实现
 *
 * 使用 LinkedList 实现双向链表
 * 支持从两端推入和弹出元素
 *
 * 面试知识点：
 * 1. 双向链表的实现
 * 2. 读写锁的使用
 * 3. 线程安全设计
 *
 * @author Mini Redis
 */
public class RedisList {

    /**
     * 底层存储结构
     * 使用 LinkedList 实现双向链表
     */
    private final LinkedList<String> list;

    /**
     * 读写锁
     * 允许多个读操作并发，写操作独占
     */
    private final ReadWriteLock lock;

    /**
     * 构造函数
     */
    public RedisList() {
        this.list = new LinkedList<>();
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * 从左侧推入元素
     *
     * @param values 要推入的元素
     * @return 推入后列表的长度
     */
    public int lpush(String... values) {
        lock.writeLock().lock();
        try {
            for (String value : values) {
                list.addFirst(value);
            }
            return list.size();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 从右侧推入元素
     *
     * @param values 要推入的元素
     * @return 推入后列表的长度
     */
    public int rpush(String... values) {
        lock.writeLock().lock();
        try {
            for (String value : values) {
                list.addLast(value);
            }
            return list.size();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 从左侧弹出元素
     *
     * @return 弹出的元素，如果列表为空返回 null
     */
    public String lpop() {
        lock.writeLock().lock();
        try {
            return list.pollFirst();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 从右侧弹出元素
     *
     * @return 弹出的元素，如果列表为空返回 null
     */
    public String rpop() {
        lock.writeLock().lock();
        try {
            return list.pollLast();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取指定范围内的元素
     *
     * @param start 起始索引（0 开始，可以为负数）
     * @param stop 结束索引（包含，可以为负数）
     * @return 指定范围内的元素列表
     */
    public List<String> lrange(long start, long stop) {
        lock.readLock().lock();
        try {
            if (list.isEmpty()) {
                return new LinkedList<>();
            }

            int size = list.size();

            // 处理负数索引
            int actualStart = normalizeIndex((int) start, size);
            int actualStop = normalizeIndex((int) stop, size);

            // 确保索引范围有效
            if (actualStart > actualStop || actualStart >= size) {
                return new LinkedList<>();
            }

            // 调整索引范围
            actualStart = Math.max(0, actualStart);
            actualStop = Math.min(size - 1, actualStop);

            // 返回子列表
            return new LinkedList<>(list.subList(actualStart, actualStop + 1));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取列表长度
     *
     * @return 列表长度
     */
    public int llen() {
        lock.readLock().lock();
        try {
            return list.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取指定索引位置的元素
     *
     * @param index 索引（可以为负数）
     * @return 元素值，如果索引超出范围返回 null
     */
    public String lindex(long index) {
        lock.readLock().lock();
        try {
            if (list.isEmpty()) {
                return null;
            }

            int actualIndex = normalizeIndex((int) index, list.size());
            if (actualIndex < 0 || actualIndex >= list.size()) {
                return null;
            }

            return list.get(actualIndex);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 设置指定索引位置的元素
     *
     * @param index 索引（可以为负数）
     * @param value 新值
     * @return 设置成功返回 true
     */
    public boolean lset(long index, String value) {
        lock.writeLock().lock();
        try {
            if (list.isEmpty()) {
                return false;
            }

            int actualIndex = normalizeIndex((int) index, list.size());
            if (actualIndex < 0 || actualIndex >= list.size()) {
                return false;
            }

            list.set(actualIndex, value);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 修剪列表，只保留指定范围内的元素
     *
     * @param start 起始索引
     * @param stop 结束索引
     */
    public void ltrim(long start, long stop) {
        lock.writeLock().lock();
        try {
            if (list.isEmpty()) {
                return;
            }

            int size = list.size();
            int actualStart = normalizeIndex((int) start, size);
            int actualStop = normalizeIndex((int) stop, size);

            // 清空列表
            if (actualStart > actualStop || actualStart >= size || actualStop < 0) {
                list.clear();
                return;
            }

            // 调整索引范围
            actualStart = Math.max(0, actualStart);
            actualStop = Math.min(size - 1, actualStop);

            // 保留指定范围的元素
            LinkedList<String> newList = new LinkedList<>(
                list.subList(actualStart, actualStop + 1)
            );
            list.clear();
            list.addAll(newList);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 规范化索引
     * 负数索引从末尾开始计算
     *
     * @param index 原始索引
     * @param size 列表大小
     * @return 规范化后的索引
     */
    private int normalizeIndex(int index, int size) {
        if (index < 0) {
            return size + index;
        }
        return index;
    }

    /**
     * 获取列表大小
     *
     * @return 列表大小
     */
    public int size() {
        return llen();
    }

    /**
     * 检查列表是否为空
     *
     * @return 如果为空返回 true
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return list.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }
}