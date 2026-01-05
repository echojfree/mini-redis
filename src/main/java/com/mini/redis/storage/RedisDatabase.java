package com.mini.redis.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis 数据库核心类
 * 负责数据存储、过期处理、内存管理等核心功能
 *
 * 面试知识点：
 * 1. ConcurrentHashMap 实现并发安全
 * 2. 过期键处理策略（定期删除 + 惰性删除）
 * 3. LRU 缓存淘汰算法
 * 4. 内存管理机制
 *
 * @author Mini Redis
 */
public class RedisDatabase {

    private static final Logger logger = LoggerFactory.getLogger(RedisDatabase.class);

    /**
     * 数据库 ID
     */
    private final int id;

    /**
     * 主键空间
     * 存储所有键值对
     */
    private final ConcurrentHashMap<String, RedisObject> keySpace;

    /**
     * 过期字典
     * 存储键的过期时间
     */
    private final ConcurrentHashMap<String, Long> expires;

    /**
     * 定期清理任务执行器
     */
    private final ScheduledExecutorService cleanupExecutor;

    /**
     * 统计信息
     */
    private final AtomicLong hitCount = new AtomicLong(0);      // 命中次数
    private final AtomicLong missCount = new AtomicLong(0);     // 未命中次数
    private final AtomicLong expiredCount = new AtomicLong(0);  // 过期键数量

    /**
     * 最大内存限制（字节）
     * 0 表示无限制
     */
    private volatile long maxMemory = 0;

    /**
     * 内存淘汰策略
     */
    private volatile EvictionPolicy evictionPolicy = EvictionPolicy.NO_EVICTION;

    /**
     * 构造函数
     *
     * @param id 数据库 ID
     */
    public RedisDatabase(int id) {
        this.id = id;
        this.keySpace = new ConcurrentHashMap<>();
        this.expires = new ConcurrentHashMap<>();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread t = new Thread(r, "redis-db-" + id + "-cleanup");
                t.setDaemon(true);
                return t;
            }
        );

        // 启动定期清理任务（每秒执行一次）
        startCleanupTask();
    }

    /**
     * 启动定期清理任务
     */
    private void startCleanupTask() {
        cleanupExecutor.scheduleWithFixedDelay(this::cleanupExpiredKeys,
            1, 1, TimeUnit.SECONDS);
    }

    /**
     * 定期清理过期键
     * Redis 的定期删除策略：每次随机检查一定数量的键
     */
    private void cleanupExpiredKeys() {
        try {
            int checked = 0;
            int deleted = 0;
            int maxCheck = 20;  // 每次最多检查 20 个键

            // 随机抽取键进行检查
            List<String> keys = new ArrayList<>(expires.keySet());
            Collections.shuffle(keys);

            for (String key : keys) {
                if (checked >= maxCheck) {
                    break;
                }

                checked++;
                if (isExpired(key)) {
                    delete(key);
                    deleted++;
                }
            }

            if (deleted > 0) {
                logger.debug("定期清理：检查 {} 个键，删除 {} 个过期键", checked, deleted);
            }

            // 如果删除比例超过 25%，立即再次执行
            if (deleted > maxCheck / 4) {
                cleanupExecutor.submit(this::cleanupExpiredKeys);
            }

        } catch (Exception e) {
            logger.error("清理过期键时发生错误", e);
        }
    }

    /**
     * 获取值
     * 实现惰性删除策略
     *
     * @param key 键
     * @return Redis 对象，如果不存在或已过期返回 null
     */
    public RedisObject get(String key) {
        // 惰性删除：访问时检查是否过期
        if (isExpired(key)) {
            delete(key);
            missCount.incrementAndGet();
            return null;
        }

        RedisObject obj = keySpace.get(key);
        if (obj != null) {
            hitCount.incrementAndGet();
        } else {
            missCount.incrementAndGet();
        }
        return obj;
    }

    /**
     * 设置值
     *
     * @param key 键
     * @param obj Redis 对象
     */
    public void set(String key, RedisObject obj) {
        // 检查内存限制
        if (shouldEvict()) {
            evict();
        }

        keySpace.put(key, obj);

        // 更新过期时间
        if (obj.getExpireTime() > 0) {
            expires.put(key, obj.getExpireTime());
        } else {
            expires.remove(key);
        }
    }

    /**
     * 删除键
     *
     * @param key 键
     * @return 如果删除成功返回 true
     */
    public boolean delete(String key) {
        expires.remove(key);
        return keySpace.remove(key) != null;
    }

    /**
     * 批量删除键
     *
     * @param keys 键列表
     * @return 删除的键数量
     */
    public int delete(String... keys) {
        int deleted = 0;
        for (String key : keys) {
            if (delete(key)) {
                deleted++;
            }
        }
        return deleted;
    }

    /**
     * 检查键是否存在
     *
     * @param key 键
     * @return 如果存在返回 true
     */
    public boolean exists(String key) {
        // 检查是否过期
        if (isExpired(key)) {
            delete(key);
            return false;
        }
        return keySpace.containsKey(key);
    }

    /**
     * 获取键的类型
     *
     * @param key 键
     * @return 数据类型
     */
    public RedisDataType type(String key) {
        RedisObject obj = get(key);
        return obj != null ? obj.getType() : RedisDataType.NONE;
    }

    /**
     * 设置过期时间
     *
     * @param key 键
     * @param seconds 过期时间（秒）
     * @return 设置成功返回 true
     */
    public boolean expire(String key, long seconds) {
        RedisObject obj = get(key);
        if (obj == null) {
            return false;
        }

        obj.setExpire(seconds, TimeUnit.SECONDS);
        if (seconds > 0) {
            expires.put(key, obj.getExpireTime());
        } else {
            expires.remove(key);
        }
        return true;
    }

    /**
     * 设置过期时间戳
     *
     * @param key 键
     * @param timestamp 过期时间戳（秒）
     * @return 设置成功返回 true
     */
    public boolean expireAt(String key, long timestamp) {
        RedisObject obj = get(key);
        if (obj == null) {
            return false;
        }

        obj.setExpireAt(timestamp * 1000);  // 转换为毫秒
        expires.put(key, obj.getExpireTime());
        return true;
    }

    /**
     * 移除过期时间
     *
     * @param key 键
     * @return 移除成功返回 true
     */
    public boolean persist(String key) {
        RedisObject obj = get(key);
        if (obj == null) {
            return false;
        }

        obj.persist();
        expires.remove(key);
        return true;
    }

    /**
     * 获取剩余生存时间（秒）
     *
     * @param key 键
     * @return TTL（秒），-1 表示永不过期，-2 表示键不存在
     */
    public long ttl(String key) {
        RedisObject obj = keySpace.get(key);  // 不使用 get() 避免删除
        if (obj == null) {
            return -2;
        }

        if (obj.isExpired()) {
            delete(key);
            return -2;
        }

        return obj.getTtlInSeconds();
    }

    /**
     * 获取剩余生存时间（毫秒）
     *
     * @param key 键
     * @return TTL（毫秒）
     */
    public long pttl(String key) {
        RedisObject obj = keySpace.get(key);
        if (obj == null) {
            return -2;
        }

        if (obj.isExpired()) {
            delete(key);
            return -2;
        }

        return obj.getTtl();
    }

    /**
     * 获取所有键（支持模式匹配）
     *
     * @param pattern 匹配模式，* 表示任意字符
     * @return 匹配的键列表
     */
    public Set<String> keys(String pattern) {
        Set<String> result = new HashSet<>();

        // 简单的模式匹配实现
        String regex = pattern.replace("*", ".*");

        for (String key : keySpace.keySet()) {
            // 检查并删除过期键
            if (isExpired(key)) {
                delete(key);
                continue;
            }

            if (key.matches(regex)) {
                result.add(key);
            }
        }

        return result;
    }

    /**
     * 随机返回一个键
     *
     * @return 随机键，如果数据库为空返回 null
     */
    public String randomKey() {
        if (keySpace.isEmpty()) {
            return null;
        }

        List<String> keys = new ArrayList<>(keySpace.keySet());
        Collections.shuffle(keys);

        for (String key : keys) {
            if (!isExpired(key)) {
                return key;
            }
        }

        return null;
    }

    /**
     * 重命名键
     *
     * @param oldKey 旧键名
     * @param newKey 新键名
     * @return 重命名成功返回 true
     */
    public boolean rename(String oldKey, String newKey) {
        if (oldKey.equals(newKey)) {
            return true;
        }

        RedisObject obj = get(oldKey);
        if (obj == null) {
            return false;
        }

        // 保留过期时间
        Long expireTime = expires.get(oldKey);

        // 删除旧键
        delete(oldKey);

        // 设置新键
        keySpace.put(newKey, obj);
        if (expireTime != null) {
            expires.put(newKey, expireTime);
        }

        return true;
    }

    /**
     * 清空数据库
     */
    public void flushDb() {
        keySpace.clear();
        expires.clear();
        logger.info("数据库 {} 已清空", id);
    }

    /**
     * 获取数据库大小
     *
     * @return 键的数量
     */
    public int dbSize() {
        // 清理过期键
        cleanupExpiredKeys();
        return keySpace.size();
    }

    /**
     * 检查键是否过期
     *
     * @param key 键
     * @return 如果过期返回 true
     */
    private boolean isExpired(String key) {
        Long expireTime = expires.get(key);
        if (expireTime == null) {
            return false;
        }

        if (System.currentTimeMillis() > expireTime) {
            expiredCount.incrementAndGet();
            return true;
        }

        return false;
    }

    /**
     * 检查是否需要淘汰
     *
     * @return 如果需要淘汰返回 true
     */
    private boolean shouldEvict() {
        if (maxMemory == 0) {
            return false;  // 无内存限制
        }

        // 简化实现：根据键数量判断
        // 实际 Redis 会计算真实的内存使用
        return keySpace.size() * 1024 > maxMemory;
    }

    /**
     * 执行内存淘汰
     */
    private void evict() {
        if (evictionPolicy == EvictionPolicy.NO_EVICTION) {
            throw new RuntimeException("OOM: 内存已满且未配置淘汰策略");
        }

        // 简化的 LRU 实现
        if (evictionPolicy == EvictionPolicy.LRU) {
            String oldestKey = null;
            long oldestTime = Long.MAX_VALUE;

            // 找出最久未使用的键
            for (Map.Entry<String, RedisObject> entry : keySpace.entrySet()) {
                RedisObject obj = entry.getValue();
                if (obj.getLastAccessTime() < oldestTime) {
                    oldestTime = obj.getLastAccessTime();
                    oldestKey = entry.getKey();
                }
            }

            if (oldestKey != null) {
                delete(oldestKey);
                logger.debug("LRU 淘汰键: {}", oldestKey);
            }
        }
    }

    /**
     * 获取数据库 ID
     *
     * @return 数据库 ID
     */
    public int getId() {
        return id;
    }

    /**
     * 获取统计信息
     *
     * @return 统计信息字符串
     */
    public String getStats() {
        long total = hitCount.get() + missCount.get();
        double hitRate = total > 0 ? (double) hitCount.get() / total * 100 : 0;

        return String.format(
            "DB %d: keys=%d, expires=%d, hit=%d, miss=%d, hit_rate=%.2f%%, expired=%d",
            id, keySpace.size(), expires.size(),
            hitCount.get(), missCount.get(), hitRate, expiredCount.get()
        );
    }

    /**
     * 关闭数据库
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 内存淘汰策略枚举
     */
    public enum EvictionPolicy {
        NO_EVICTION,    // 不淘汰
        LRU,            // 最近最少使用
        LFU,            // 最不经常使用
        RANDOM,         // 随机淘汰
        TTL             // 淘汰即将过期的键
    }
}