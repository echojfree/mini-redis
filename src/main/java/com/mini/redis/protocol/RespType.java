package com.mini.redis.protocol;

/**
 * RESP (REdis Serialization Protocol) 数据类型枚举
 *
 * RESP 协议定义了 5 种数据类型：
 * 1. Simple String - 简单字符串，以 + 开头
 * 2. Error - 错误消息，以 - 开头
 * 3. Integer - 整数，以 : 开头
 * 4. Bulk String - 批量字符串，以 $ 开头
 * 5. Array - 数组，以 * 开头
 *
 * @author Mini Redis
 */
public enum RespType {

    /**
     * 简单字符串
     * 格式：+OK\r\n
     * 用于状态回复
     */
    SIMPLE_STRING('+'),

    /**
     * 错误消息
     * 格式：-Error message\r\n
     * 用于返回错误
     */
    ERROR('-'),

    /**
     * 整数
     * 格式：:1000\r\n
     * 用于返回数字
     */
    INTEGER(':'),

    /**
     * 批量字符串
     * 格式：$6\r\nfoobar\r\n
     * 用于返回二进制安全的字符串
     */
    BULK_STRING('$'),

    /**
     * 数组
     * 格式：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
     * 用于返回多个值
     */
    ARRAY('*');

    /**
     * 类型前缀字符
     */
    private final char prefix;

    /**
     * 构造函数
     *
     * @param prefix 类型前缀字符
     */
    RespType(char prefix) {
        this.prefix = prefix;
    }

    /**
     * 获取类型前缀字符
     *
     * @return 前缀字符
     */
    public char getPrefix() {
        return prefix;
    }

    /**
     * 根据前缀字符获取类型
     *
     * @param prefix 前缀字符
     * @return 对应的 RESP 类型，如果未知返回 null
     */
    public static RespType fromPrefix(char prefix) {
        for (RespType type : values()) {
            if (type.prefix == prefix) {
                return type;
            }
        }
        return null;
    }

    /**
     * 根据字节前缀获取类型
     *
     * @param prefix 前缀字节
     * @return 对应的 RESP 类型，如果未知返回 null
     */
    public static RespType fromPrefix(byte prefix) {
        return fromPrefix((char) prefix);
    }
}