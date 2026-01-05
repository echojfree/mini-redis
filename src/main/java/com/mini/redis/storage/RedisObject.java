package com.mini.redis.storage;

import java.util.concurrent.TimeUnit;

/**
 * Redis 对象封装
 * 封装存储的值和元数据（类型、过期时间等）
 *
 * 知识点：
 * 1. 对象封装模式
 * 2. 过期时间管理
 * 3. LRU/LFU 访问统计
 *
 * @author Mini Redis
 */
public class RedisObject {

    /**
     * 数据类型
     */
    private final RedisDataType type;

    /**
     * 实际存储的值
     */
    private volatile Object value;

    /**
     * 过期时间戳（毫秒）
     * -1 表示永不过期
     */
    private volatile long expireTime = -1;

    /**
     * 最后访问时间（用于 LRU）
     */
    private volatile long lastAccessTime;

    /**
     * 访问次数（用于 LFU）
     */
    private volatile int accessCount;

    /**
     * 创建时间
     */
    private final long createTime;

    /**
     * 构造函数
     *
     * @param type 数据类型
     * @param value 存储的值
     */
    public RedisObject(RedisDataType type, Object value) {
        this.type = type;
        this.value = value;
        this.createTime = System.currentTimeMillis();
        this.lastAccessTime = this.createTime;
        this.accessCount = 0;
    }

    /**
     * 获取数据类型
     *
     * @return 数据类型
     */
    public RedisDataType getType() {
        return type;
    }

    /**
     * 获取存储的值
     *
     * @return 存储的值
     */
    public Object getValue() {
        // 更新访问统计
        this.lastAccessTime = System.currentTimeMillis();
        this.accessCount++;
        return value;
    }

    /**
     * 设置存储的值
     *
     * @param value 新值
     */
    public void setValue(Object value) {
        this.value = value;
        // 更新访问时间
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * 设置过期时间
     *
     * @param ttl 存活时间
     * @param unit 时间单位
     */
    public void setExpire(long ttl, TimeUnit unit) {
        if (ttl > 0) {
            this.expireTime = System.currentTimeMillis() + unit.toMillis(ttl);
        } else {
            this.expireTime = -1;  // 永不过期
        }
    }

    /**
     * 设置过期时间戳
     *
     * @param expireTime 过期时间戳（毫秒）
     */
    public void setExpireAt(long expireTime) {
        this.expireTime = expireTime;
    }

    /**
     * 移除过期时间
     */
    public void persist() {
        this.expireTime = -1;
    }

    /**
     * 检查是否已过期
     *
     * @return 如果已过期返回 true
     */
    public boolean isExpired() {
        if (expireTime == -1) {
            return false;  // 永不过期
        }
        return System.currentTimeMillis() > expireTime;
    }

    /**
     * 获取剩余存活时间（毫秒）
     *
     * @return 剩余时间，如果已过期返回负数，永不过期返回 -1
     */
    public long getTtl() {
        if (expireTime == -1) {
            return -1;  // 永不过期
        }

        long ttl = expireTime - System.currentTimeMillis();
        return ttl > 0 ? ttl : -2;  // -2 表示键已过期
    }

    /**
     * 获取剩余存活时间（秒）
     *
     * @return 剩余时间（秒）
     */
    public long getTtlInSeconds() {
        long ttl = getTtl();
        if (ttl == -1 || ttl == -2) {
            return ttl;
        }
        return ttl / 1000;
    }

    /**
     * 获取过期时间戳
     *
     * @return 过期时间戳，-1 表示永不过期
     */
    public long getExpireTime() {
        return expireTime;
    }

    /**
     * 获取最后访问时间
     *
     * @return 最后访问时间戳
     */
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    /**
     * 获取访问次数
     *
     * @return 访问次数
     */
    public int getAccessCount() {
        return accessCount;
    }

    /**
     * 获取创建时间
     *
     * @return 创建时间戳
     */
    public long getCreateTime() {
        return createTime;
    }

    /**
     * 获取对象年龄（毫秒）
     *
     * @return 对象存在时间
     */
    public long getAge() {
        return System.currentTimeMillis() - createTime;
    }

    @Override
    public String toString() {
        return "RedisObject{" +
                "type=" + type +
                ", value=" + value +
                ", expireTime=" + expireTime +
                ", ttl=" + getTtl() +
                ", accessCount=" + accessCount +
                '}';
    }
}