package com.mini.redis.command;

import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;

import java.util.List;

/**
 * Redis 命令接口
 * 所有 Redis 命令的统一接口
 *
 * 设计模式：命令模式（Command Pattern）
 * 每个具体的 Redis 命令都实现这个接口
 *
 * @author Mini Redis
 */
public interface Command {

    /**
     * 执行命令
     *
     * @param args 命令参数列表
     * @param client 客户端连接
     * @return 命令执行结果
     */
    RespMessage execute(List<String> args, RedisClient client);

    /**
     * 获取命令名称
     *
     * @return 命令名称（大写）
     */
    String getName();

    /**
     * 验证参数数量是否正确
     *
     * @param argCount 实际参数数量
     * @return 如果参数数量正确返回 true
     */
    boolean validateArgCount(int argCount);

    /**
     * 获取命令的最小参数数量
     *
     * @return 最小参数数量
     */
    default int getMinArgs() {
        return 0;
    }

    /**
     * 获取命令的最大参数数量
     *
     * @return 最大参数数量，-1 表示无限制
     */
    default int getMaxArgs() {
        return -1;
    }

    /**
     * 获取命令描述
     *
     * @return 命令描述
     */
    default String getDescription() {
        return "No description available";
    }

    /**
     * 获取命令用法示例
     *
     * @return 用法示例
     */
    default String getUsage() {
        return getName();
    }
}