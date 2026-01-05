package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;

import java.util.List;

/**
 * TYPE 命令实现
 *
 * 功能：返回键的数据类型
 * 语法：TYPE key
 *
 * 返回值：
 * - string: 字符串
 * - list: 列表
 * - hash: 哈希表
 * - set: 集合
 * - zset: 有序集合
 * - none: 键不存在
 *
 * @author Mini Redis
 */
public class TypeCommand implements Command {

    private static final String NAME = "TYPE";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.size() != 1) {
            return RespMessage.error("wrong number of arguments for 'type' command");
        }

        String key = args.get(0);
        RedisDatabase db = client.getCurrentDatabase();

        // 获取键的类型
        RedisDataType type = db.type(key);

        // 返回类型名称
        return new RespMessage.SimpleString(type.getName());
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean validateArgCount(int argCount) {
        return argCount == 1;
    }

    @Override
    public String getDescription() {
        return "返回键的数据类型";
    }

    @Override
    public String getUsage() {
        return "TYPE key";
    }
}