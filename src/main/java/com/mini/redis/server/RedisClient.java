package com.mini.redis.server;

import com.mini.redis.storage.DatabaseManager;
import com.mini.redis.storage.RedisDatabase;
import io.netty.channel.Channel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis 客户端连接
 * 管理每个客户端连接的状态信息
 *
 * 包括：
 * - 当前选择的数据库
 * - 连接信息
 * - 事务状态
 * - 认证状态等
 *
 * @author Mini Redis
 */
public class RedisClient {

    /**
     * 客户端 ID 生成器
     */
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);

    /**
     * 所有活跃的客户端连接
     */
    private static final ConcurrentHashMap<Integer, RedisClient> CLIENTS = new ConcurrentHashMap<>();

    /**
     * 客户端 ID
     */
    private final int id;

    /**
     * Netty 通道
     */
    private final Channel channel;

    /**
     * 当前选择的数据库索引
     */
    private volatile int currentDatabase = 0;

    /**
     * 客户端地址
     */
    private final String address;

    /**
     * 连接时间
     */
    private final long connectTime;

    /**
     * 最后活动时间
     */
    private volatile long lastActiveTime;

    /**
     * 执行的命令数量
     */
    private final AtomicInteger commandCount = new AtomicInteger(0);

    /**
     * 是否已认证（如果启用密码）
     */
    private volatile boolean authenticated = true;  // 默认不需要认证

    /**
     * 事务状态
     */
    private volatile boolean inTransaction = false;

    /**
     * 客户端名称（可选）
     */
    private volatile String name;

    /**
     * 构造函数
     *
     * @param channel Netty 通道
     */
    public RedisClient(Channel channel) {
        this.id = ID_GENERATOR.incrementAndGet();
        this.channel = channel;
        this.address = channel.remoteAddress() != null
            ? channel.remoteAddress().toString()
            : "mock-client-" + id;
        this.connectTime = System.currentTimeMillis();
        this.lastActiveTime = this.connectTime;

        // 注册客户端
        CLIENTS.put(id, this);
    }

    /**
     * 获取当前数据库
     *
     * @return 当前数据库
     */
    public RedisDatabase getCurrentDatabase() {
        return DatabaseManager.getInstance().getDatabase(currentDatabase);
    }

    /**
     * 切换数据库
     *
     * @param index 数据库索引
     * @return 切换成功返回 true
     */
    public boolean selectDatabase(int index) {
        try {
            // 验证数据库索引
            DatabaseManager.getInstance().getDatabase(index);
            this.currentDatabase = index;
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * 更新活动时间
     */
    public void updateActiveTime() {
        this.lastActiveTime = System.currentTimeMillis();
        this.commandCount.incrementAndGet();
    }

    /**
     * 获取客户端 ID
     *
     * @return 客户端 ID
     */
    public int getId() {
        return id;
    }

    /**
     * 获取通道
     *
     * @return Netty 通道
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * 获取当前数据库索引
     *
     * @return 数据库索引
     */
    public int getCurrentDatabaseIndex() {
        return currentDatabase;
    }

    /**
     * 获取客户端地址
     *
     * @return 客户端地址
     */
    public String getAddress() {
        return address;
    }

    /**
     * 获取连接时长（秒）
     *
     * @return 连接时长
     */
    public long getAge() {
        return (System.currentTimeMillis() - connectTime) / 1000;
    }

    /**
     * 获取空闲时长（秒）
     *
     * @return 空闲时长
     */
    public long getIdleTime() {
        return (System.currentTimeMillis() - lastActiveTime) / 1000;
    }

    /**
     * 获取执行的命令数量
     *
     * @return 命令数量
     */
    public int getCommandCount() {
        return commandCount.get();
    }

    /**
     * 是否已认证
     *
     * @return 如果已认证返回 true
     */
    public boolean isAuthenticated() {
        return authenticated;
    }

    /**
     * 设置认证状态
     *
     * @param authenticated 是否已认证
     */
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    /**
     * 是否在事务中
     *
     * @return 如果在事务中返回 true
     */
    public boolean isInTransaction() {
        return inTransaction;
    }

    /**
     * 设置事务状态
     *
     * @param inTransaction 是否在事务中
     */
    public void setInTransaction(boolean inTransaction) {
        this.inTransaction = inTransaction;
    }

    /**
     * 获取客户端名称
     *
     * @return 客户端名称
     */
    public String getName() {
        return name != null ? name : "";
    }

    /**
     * 设置客户端名称
     *
     * @param name 客户端名称
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 断开连接时清理
     */
    public void disconnect() {
        CLIENTS.remove(id);
    }

    /**
     * 获取客户端信息字符串
     *
     * @return 客户端信息
     */
    public String getInfo() {
        return String.format(
            "id=%d addr=%s name=%s age=%d idle=%d db=%d cmd=%d",
            id, address, getName(), getAge(), getIdleTime(),
            currentDatabase, commandCount.get()
        );
    }

    /**
     * 获取所有客户端
     *
     * @return 客户端集合
     */
    public static ConcurrentHashMap<Integer, RedisClient> getAllClients() {
        return new ConcurrentHashMap<>(CLIENTS);
    }

    /**
     * 根据通道获取客户端
     *
     * @param channel Netty 通道
     * @return 客户端实例，如果不存在返回 null
     */
    public static RedisClient getClient(Channel channel) {
        for (RedisClient client : CLIENTS.values()) {
            if (client.channel == channel) {
                return client;
            }
        }
        return null;
    }

    /**
     * 获取客户端数量
     *
     * @return 客户端数量
     */
    public static int getClientCount() {
        return CLIENTS.size();
    }

    @Override
    public String toString() {
        return "RedisClient{" +
                "id=" + id +
                ", address='" + address + '\'' +
                ", database=" + currentDatabase +
                ", commands=" + commandCount.get() +
                '}';
    }
}