package com.mini.redis.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Redis 配置管理器
 * 负责加载和管理所有配置项
 *
 * 设计模式：单例模式
 * 线程安全：使用 ConcurrentHashMap 保证线程安全
 *
 * @author Mini Redis
 */
public class RedisConfig {

    private static final Logger logger = LoggerFactory.getLogger(RedisConfig.class);

    /**
     * 配置文件名
     */
    private static final String CONFIG_FILE = "redis.properties";

    /**
     * 单例实例
     */
    private static volatile RedisConfig instance;

    /**
     * 配置项存储
     * 使用 ConcurrentHashMap 保证线程安全
     */
    private final ConcurrentHashMap<String, String> configs;

    /**
     * 私有构造函数，防止外部实例化
     */
    private RedisConfig() {
        this.configs = new ConcurrentHashMap<>();
        loadConfig();
    }

    /**
     * 获取配置管理器单例
     * 使用双重检查锁定（Double-Check Locking）模式
     *
     * @return 配置管理器实例
     */
    public static RedisConfig getInstance() {
        if (instance == null) {
            synchronized (RedisConfig.class) {
                if (instance == null) {
                    instance = new RedisConfig();
                }
            }
        }
        return instance;
    }

    /**
     * 加载配置文件
     */
    private void loadConfig() {
        Properties properties = new Properties();

        try (InputStream inputStream = getClass().getClassLoader()
                .getResourceAsStream(CONFIG_FILE)) {

            if (inputStream == null) {
                logger.warn("配置文件 {} 未找到，使用默认配置", CONFIG_FILE);
                loadDefaultConfig();
                return;
            }

            properties.load(inputStream);

            // 将 Properties 转换为 ConcurrentHashMap
            properties.forEach((key, value) ->
                configs.put(String.valueOf(key), String.valueOf(value))
            );

            logger.info("成功加载配置文件，共 {} 个配置项", configs.size());

        } catch (IOException e) {
            logger.error("加载配置文件失败", e);
            loadDefaultConfig();
        }
    }

    /**
     * 加载默认配置
     */
    private void loadDefaultConfig() {
        // 服务器配置
        configs.put("server.port", "6379");
        configs.put("server.bind", "0.0.0.0");
        configs.put("server.backlog", "128");
        configs.put("server.timeout", "0");
        configs.put("server.tcp-keepalive", "300");

        // 网络配置
        configs.put("network.io-threads", "4");
        configs.put("network.worker-threads", "8");
        configs.put("network.so-backlog", "511");
        configs.put("network.tcp-nodelay", "true");

        // 存储配置
        configs.put("storage.databases", "16");
        configs.put("storage.maxmemory", "0");
        configs.put("storage.maxmemory-policy", "noeviction");

        // 持久化配置
        configs.put("rdb.enabled", "true");
        configs.put("rdb.filename", "dump.rdb");
        configs.put("aof.enabled", "false");
        configs.put("aof.filename", "appendonly.aof");

        // 客户端配置
        configs.put("client.max-clients", "10000");
        configs.put("client.output-buffer-limit", "0");

        logger.info("使用默认配置");
    }

    /**
     * 获取字符串配置
     *
     * @param key 配置键
     * @return 配置值，如果不存在返回 null
     */
    public String getString(String key) {
        return configs.get(key);
    }

    /**
     * 获取字符串配置，带默认值
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 配置值，如果不存在返回默认值
     */
    public String getString(String key, String defaultValue) {
        return configs.getOrDefault(key, defaultValue);
    }

    /**
     * 获取整型配置
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 配置值，如果不存在或解析失败返回默认值
     */
    public int getInt(String key, int defaultValue) {
        String value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            logger.warn("配置项 {} 的值 {} 不是有效的整数，使用默认值 {}",
                key, value, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 获取长整型配置
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 配置值，如果不存在或解析失败返回默认值
     */
    public long getLong(String key, long defaultValue) {
        String value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            logger.warn("配置项 {} 的值 {} 不是有效的长整数，使用默认值 {}",
                key, value, defaultValue);
            return defaultValue;
        }
    }

    /**
     * 获取布尔配置
     *
     * @param key 配置键
     * @param defaultValue 默认值
     * @return 配置值，如果不存在返回默认值
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        String value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }

        return "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    /**
     * 设置配置项
     *
     * @param key 配置键
     * @param value 配置值
     */
    public void set(String key, String value) {
        if (key != null && value != null) {
            String oldValue = configs.put(key, value);
            logger.info("更新配置项: {} = {} (原值: {})", key, value, oldValue);
        }
    }

    /**
     * 获取服务器端口
     *
     * @return 端口号
     */
    public int getServerPort() {
        return getInt("server.port", 6379);
    }

    /**
     * 获取服务器绑定地址
     *
     * @return 绑定地址
     */
    public String getServerBind() {
        return getString("server.bind", "0.0.0.0");
    }

    /**
     * 获取 IO 线程数
     *
     * @return IO 线程数
     */
    public int getIoThreads() {
        return getInt("network.io-threads", 4);
    }

    /**
     * 获取工作线程数
     *
     * @return 工作线程数
     */
    public int getWorkerThreads() {
        return getInt("network.worker-threads", 8);
    }

    /**
     * 获取数据库数量
     *
     * @return 数据库数量
     */
    public int getDatabaseCount() {
        return getInt("storage.databases", 16);
    }

    /**
     * 是否启用 TCP NoDelay
     *
     * @return 是否启用
     */
    public boolean isTcpNoDelay() {
        return getBoolean("network.tcp-nodelay", true);
    }

    /**
     * 获取最大客户端连接数
     *
     * @return 最大客户端连接数
     */
    public int getMaxClients() {
        return getInt("client.max-clients", 10000);
    }

    /**
     * 打印所有配置项（用于调试）
     */
    public void printAllConfigs() {
        logger.info("=== 当前配置项 ===");
        configs.forEach((key, value) ->
            logger.info("{} = {}", key, value)
        );
        logger.info("==================");
    }
}