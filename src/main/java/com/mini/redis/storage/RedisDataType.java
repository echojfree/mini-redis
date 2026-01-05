package com.mini.redis.storage;

/**
 * Redis 数据类型枚举
 * 定义 Redis 支持的五大数据类型
 *
 * @author Mini Redis
 */
public enum RedisDataType {

    /**
     * 字符串类型
     * 最基础的数据类型，可以存储字符串、整数或浮点数
     */
    STRING("string"),

    /**
     * 列表类型
     * 双向链表，支持从两端推入和弹出元素
     */
    LIST("list"),

    /**
     * 哈希类型
     * 键值对集合，适合存储对象
     */
    HASH("hash"),

    /**
     * 集合类型
     * 无序不重复元素集合
     */
    SET("set"),

    /**
     * 有序集合类型
     * 带分数的有序集合，自动排序
     */
    ZSET("zset"),

    /**
     * 无类型（键不存在）
     */
    NONE("none");

    /**
     * 类型名称
     */
    private final String name;

    /**
     * 构造函数
     *
     * @param name 类型名称
     */
    RedisDataType(String name) {
        this.name = name;
    }

    /**
     * 获取类型名称
     *
     * @return 类型名称
     */
    public String getName() {
        return name;
    }

    /**
     * 根据名称获取类型
     *
     * @param name 类型名称
     * @return 对应的数据类型，如果未找到返回 NONE
     */
    public static RedisDataType fromName(String name) {
        if (name == null) {
            return NONE;
        }

        for (RedisDataType type : values()) {
            if (type.name.equalsIgnoreCase(name)) {
                return type;
            }
        }
        return NONE;
    }
}