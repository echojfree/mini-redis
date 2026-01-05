package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;

import java.util.List;

/**
 * DEL 命令实现
 *
 * 功能：删除一个或多个键
 * 语法：DEL key [key ...]
 * 返回：被删除键的数量
 *
 * @author Mini Redis
 */
public class DelCommand implements Command {

    private static final String NAME = "DEL";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.isEmpty()) {
            return RespMessage.error("wrong number of arguments for 'del' command");
        }

        RedisDatabase db = client.getCurrentDatabase();

        // 删除所有指定的键
        int deleted = 0;
        for (String key : args) {
            if (db.delete(key)) {
                deleted++;
            }
        }

        // 返回删除的键数量
        return new RespMessage.Integer(deleted);
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
        return -1;  // 可以删除多个键
    }

    @Override
    public String getDescription() {
        return "删除一个或多个键";
    }

    @Override
    public String getUsage() {
        return "DEL key [key ...]";
    }
}