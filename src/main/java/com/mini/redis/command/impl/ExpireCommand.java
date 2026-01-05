package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;

import java.util.List;

/**
 * EXPIRE 命令实现
 *
 * 功能：设置键的过期时间（秒）
 * 语法：EXPIRE key seconds
 *
 * @author Mini Redis
 */
public class ExpireCommand implements Command {

    private static final String NAME = "EXPIRE";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.size() != 2) {
            return RespMessage.error("wrong number of arguments for 'expire' command");
        }

        String key = args.get(0);

        // 解析过期时间
        long seconds;
        try {
            seconds = Long.parseLong(args.get(1));
        } catch (NumberFormatException e) {
            return RespMessage.error("value is not an integer or out of range");
        }

        RedisDatabase db = client.getCurrentDatabase();

        // 设置过期时间
        boolean success = db.expire(key, seconds);

        // 返回结果（1 表示设置成功，0 表示键不存在）
        return new RespMessage.Integer(success ? 1 : 0);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean validateArgCount(int argCount) {
        return argCount == 2;
    }

    @Override
    public String getDescription() {
        return "设置键的过期时间（秒）";
    }

    @Override
    public String getUsage() {
        return "EXPIRE key seconds";
    }
}