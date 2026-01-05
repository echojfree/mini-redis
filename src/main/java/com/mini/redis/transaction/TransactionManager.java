package com.mini.redis.transaction;

import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis 事务管理器
 *
 * 实现 Redis 的事务功能：
 * - MULTI：开启事务
 * - EXEC：执行事务
 * - DISCARD：放弃事务
 * - WATCH：乐观锁监视
 * - UNWATCH：取消监视
 *
 * 面试知识点：
 * 1. 事务的 ACID 特性（Redis 只保证隔离性和一致性）
 * 2. 命令队列的实现
 * 3. 乐观锁（Optimistic Locking）机制
 * 4. CAS（Compare And Swap）操作
 * 5. 事务回滚机制（Redis 不支持回滚）
 *
 * @author Mini Redis
 */
public class TransactionManager {

    /**
     * 事务状态
     */
    public enum TransactionState {
        NONE,       // 无事务
        QUEUING,    // 命令入队中
        EXECUTING,  // 执行中
        DISCARDED   // 已放弃
    }

    /**
     * 事务上下文
     * 保存每个客户端的事务状态
     */
    public static class TransactionContext {
        // 事务状态
        private volatile TransactionState state = TransactionState.NONE;

        // 命令队列
        private final List<QueuedCommand> commandQueue = new ArrayList<>();

        // 监视的 key 集合
        private final Set<String> watchedKeys = new HashSet<>();

        // 监视的 key 版本号（用于检测修改）
        private final Map<String, Long> watchedVersions = new HashMap<>();

        // 事务是否被打断（WATCH 的 key 被修改）
        private volatile boolean aborted = false;

        /**
         * 开始事务
         */
        public void begin() {
            if (state != TransactionState.NONE) {
                throw new IllegalStateException("Transaction already started");
            }
            state = TransactionState.QUEUING;
            commandQueue.clear();
            aborted = false;
        }

        /**
         * 将命令加入队列
         */
        public void enqueue(RespMessage command) {
            if (state != TransactionState.QUEUING) {
                throw new IllegalStateException("Not in transaction");
            }
            commandQueue.add(new QueuedCommand(command));
        }

        /**
         * 执行事务
         */
        public List<QueuedCommand> execute() {
            if (state != TransactionState.QUEUING) {
                throw new IllegalStateException("Not in transaction");
            }

            if (aborted) {
                // 事务被打断，返回空
                state = TransactionState.DISCARDED;
                return null;
            }

            state = TransactionState.EXECUTING;
            List<QueuedCommand> commands = new ArrayList<>(commandQueue);

            // 清理事务状态
            reset();

            return commands;
        }

        /**
         * 放弃事务
         */
        public void discard() {
            if (state != TransactionState.QUEUING) {
                throw new IllegalStateException("Not in transaction");
            }
            state = TransactionState.DISCARDED;
            reset();
        }

        /**
         * 监视 key
         */
        public void watch(String key, Long version) {
            watchedKeys.add(key);
            watchedVersions.put(key, version);
        }

        /**
         * 取消所有监视
         */
        public void unwatch() {
            watchedKeys.clear();
            watchedVersions.clear();
        }

        /**
         * 检查被监视的 key 是否被修改
         */
        public void checkWatched(String key, Long currentVersion) {
            if (watchedKeys.contains(key)) {
                Long watchedVersion = watchedVersions.get(key);
                if (watchedVersion != null && !watchedVersion.equals(currentVersion)) {
                    // key 被修改，事务需要被打断
                    aborted = true;
                }
            }
        }

        /**
         * 重置事务状态
         */
        private void reset() {
            state = TransactionState.NONE;
            commandQueue.clear();
            unwatch();
            aborted = false;
        }

        // Getters
        public TransactionState getState() {
            return state;
        }

        public boolean isInTransaction() {
            return state == TransactionState.QUEUING;
        }

        public boolean isAborted() {
            return aborted;
        }

        public int getQueueSize() {
            return commandQueue.size();
        }

        public Set<String> getWatchedKeys() {
            return new HashSet<>(watchedKeys);
        }
    }

    /**
     * 队列中的命令
     */
    public static class QueuedCommand {
        private final RespMessage command;
        private final long timestamp;

        public QueuedCommand(RespMessage command) {
            this.command = command;
            this.timestamp = System.currentTimeMillis();
        }

        public RespMessage getCommand() {
            return command;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * 全局版本号管理器
     * 用于 WATCH 机制
     */
    private static class VersionManager {
        private final ConcurrentHashMap<String, Long> versions = new ConcurrentHashMap<>();

        /**
         * 获取 key 的版本号
         */
        public Long getVersion(String key) {
            return versions.get(key);
        }

        /**
         * 更新 key 的版本号
         */
        public void updateVersion(String key) {
            versions.compute(key, (k, v) -> v == null ? 1L : v + 1);
        }

        /**
         * 删除 key 的版本号
         */
        public void removeVersion(String key) {
            versions.remove(key);
        }
    }

    // 单例实例
    private static volatile TransactionManager instance;

    // 客户端事务上下文映射
    private final ConcurrentHashMap<RedisClient, TransactionContext> contexts;

    // 全局版本管理器
    private final VersionManager versionManager;

    /**
     * 私有构造函数
     */
    private TransactionManager() {
        this.contexts = new ConcurrentHashMap<>();
        this.versionManager = new VersionManager();
    }

    /**
     * 获取单例实例
     */
    public static TransactionManager getInstance() {
        if (instance == null) {
            synchronized (TransactionManager.class) {
                if (instance == null) {
                    instance = new TransactionManager();
                }
            }
        }
        return instance;
    }

    /**
     * 获取客户端的事务上下文
     */
    public TransactionContext getContext(RedisClient client) {
        return contexts.computeIfAbsent(client, k -> new TransactionContext());
    }

    /**
     * 移除客户端的事务上下文
     */
    public void removeContext(RedisClient client) {
        contexts.remove(client);
    }

    /**
     * 开始事务
     */
    public void multi(RedisClient client) {
        TransactionContext context = getContext(client);
        context.begin();
    }

    /**
     * 执行事务
     */
    public List<QueuedCommand> exec(RedisClient client) {
        TransactionContext context = getContext(client);
        return context.execute();
    }

    /**
     * 放弃事务
     */
    public void discard(RedisClient client) {
        TransactionContext context = getContext(client);
        context.discard();
    }

    /**
     * 监视 key
     */
    public void watch(RedisClient client, String key) {
        TransactionContext context = getContext(client);
        Long version = versionManager.getVersion(key);
        context.watch(key, version);
    }

    /**
     * 取消监视
     */
    public void unwatch(RedisClient client) {
        TransactionContext context = getContext(client);
        context.unwatch();
    }

    /**
     * 将命令加入队列
     */
    public void enqueue(RedisClient client, RespMessage command) {
        TransactionContext context = getContext(client);
        context.enqueue(command);
    }

    /**
     * 检查客户端是否在事务中
     */
    public boolean isInTransaction(RedisClient client) {
        TransactionContext context = contexts.get(client);
        return context != null && context.isInTransaction();
    }

    /**
     * 通知 key 被修改（用于 WATCH 机制）
     */
    public void notifyKeyModified(String key) {
        // 更新版本号
        versionManager.updateVersion(key);

        // 检查所有客户端的监视
        for (Map.Entry<RedisClient, TransactionContext> entry : contexts.entrySet()) {
            TransactionContext context = entry.getValue();
            if (context.isInTransaction()) {
                Long currentVersion = versionManager.getVersion(key);
                context.checkWatched(key, currentVersion);
            }
        }
    }

    /**
     * 通知 key 被删除
     */
    public void notifyKeyDeleted(String key) {
        versionManager.removeVersion(key);
        notifyKeyModified(key);  // 删除也视为修改
    }

    /**
     * 获取全局统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("active_transactions", contexts.values().stream()
            .filter(TransactionContext::isInTransaction).count());
        stats.put("total_clients", contexts.size());
        stats.put("watched_keys_count", contexts.values().stream()
            .mapToInt(c -> c.getWatchedKeys().size()).sum());
        return stats;
    }

    /**
     * 清理所有事务（用于测试）
     */
    void clearAll() {
        contexts.clear();
    }
}