package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;

import java.util.List;

/**
 * TTL 命令实现
 *
 * 功能：返回键的剩余生存时间（秒）
 * 语法：TTL key
 *
 * 返回值：
 * - 正数：剩余秒数
 * - -1：键存在但没有设置过期时间
 * - -2：键不存在
 *
 * @author Mini Redis
 */
public class TtlCommand implements Command {

    private static final String NAME = "TTL";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.size() != 1) {
            return RespMessage.error("wrong number of arguments for 'ttl' command");
        }

        String key = args.get(0);
        RedisDatabase db = client.getCurrentDatabase();

        // 获取 TTL
        long ttl = db.ttl(key);

        return new RespMessage.Integer(ttl);
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
        return "返回键的剩余生存时间（秒）";
    }

    @Override
    public String getUsage() {
        return "TTL key";
    }
}