package com.mini.redis.storage;

import com.mini.redis.config.RedisConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis 数据库管理器
 * 管理多个数据库实例，提供数据库切换功能
 *
 * Redis 默认支持 16 个数据库（0-15）
 * 使用单例模式管理所有数据库
 *
 * @author Mini Redis
 */
public class DatabaseManager {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);

    /**
     * 单例实例
     */
    private static volatile DatabaseManager instance;

    /**
     * 数据库集合
     */
    private final ConcurrentHashMap<Integer, RedisDatabase> databases;

    /**
     * 数据库数量
     */
    private final int databaseCount;

    /**
     * 当前活跃的数据库数量
     */
    private final AtomicInteger activeDatabaseCount = new AtomicInteger(0);

    /**
     * 私有构造函数
     */
    private DatabaseManager() {
        RedisConfig config = RedisConfig.getInstance();
        this.databaseCount = config.getDatabaseCount();
        this.databases = new ConcurrentHashMap<>(databaseCount);

        // 初始化数据库
        for (int i = 0; i < databaseCount; i++) {
            databases.put(i, new RedisDatabase(i));
            activeDatabaseCount.incrementAndGet();
        }

        logger.info("数据库管理器初始化完成，共 {} 个数据库", databaseCount);
    }

    /**
     * 获取单例实例
     *
     * @return 数据库管理器实例
     */
    public static DatabaseManager getInstance() {
        if (instance == null) {
            synchronized (DatabaseManager.class) {
                if (instance == null) {
                    instance = new DatabaseManager();
                }
            }
        }
        return instance;
    }

    /**
     * 获取指定的数据库
     *
     * @param index 数据库索引
     * @return 数据库实例
     * @throws IllegalArgumentException 如果索引超出范围
     */
    public RedisDatabase getDatabase(int index) {
        if (index < 0 || index >= databaseCount) {
            throw new IllegalArgumentException(
                "数据库索引超出范围: " + index + " (范围: 0-" + (databaseCount - 1) + ")");
        }

        return databases.get(index);
    }

    /**
     * 获取默认数据库（0 号数据库）
     *
     * @return 默认数据库
     */
    public RedisDatabase getDefaultDatabase() {
        return getDatabase(0);
    }

    /**
     * 清空指定数据库
     *
     * @param index 数据库索引
     */
    public void flushDb(int index) {
        RedisDatabase db = getDatabase(index);
        db.flushDb();
        logger.info("数据库 {} 已清空", index);
    }

    /**
     * 清空所有数据库
     */
    public void flushAll() {
        for (RedisDatabase db : databases.values()) {
            db.flushDb();
        }
        logger.info("所有数据库已清空");
    }

    /**
     * 获取数据库数量
     *
     * @return 数据库数量
     */
    public int getDatabaseCount() {
        return databaseCount;
    }

    /**
     * 获取所有数据库的键总数
     *
     * @return 键总数
     */
    public int getTotalKeyCount() {
        int total = 0;
        for (RedisDatabase db : databases.values()) {
            total += db.dbSize();
        }
        return total;
    }

    /**
     * 获取统计信息
     *
     * @return 统计信息字符串
     */
    public String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== 数据库统计信息 ===\n");
        sb.append("数据库数量: ").append(databaseCount).append("\n");
        sb.append("总键数: ").append(getTotalKeyCount()).append("\n");
        sb.append("\n");

        for (RedisDatabase db : databases.values()) {
            sb.append(db.getStats()).append("\n");
        }

        return sb.toString();
    }

    /**
     * 关闭所有数据库
     */
    public void shutdown() {
        logger.info("正在关闭所有数据库...");
        for (RedisDatabase db : databases.values()) {
            db.shutdown();
        }
        logger.info("所有数据库已关闭");
    }
}