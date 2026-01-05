package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;

import java.util.List;

/**
 * EXISTS 命令实现
 *
 * 功能：检查键是否存在
 * 语法：EXISTS key [key ...]
 * 返回：存在的键的数量
 *
 * @author Mini Redis
 */
public class ExistsCommand implements Command {

    private static final String NAME = "EXISTS";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.isEmpty()) {
            return RespMessage.error("wrong number of arguments for 'exists' command");
        }

        RedisDatabase db = client.getCurrentDatabase();

        // 计算存在的键数量
        int count = 0;
        for (String key : args) {
            if (db.exists(key)) {
                count++;
            }
        }

        return new RespMessage.Integer(count);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean validateArgCount(int argCount) {
        return argCount >= 1;
    }

    @Override
    public int getMinArgs() {
        return 1;
    }

    @Override
    public int getMaxArgs() {
        return -1;
    }

    @Override
    public String getDescription() {
        return "检查键是否存在";
    }

    @Override
    public String getUsage() {
        return "EXISTS key [key ...]";
    }
}