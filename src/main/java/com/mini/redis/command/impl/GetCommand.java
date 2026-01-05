package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;

import java.util.List;

/**
 * GET 命令实现
 *
 * 功能：获取键的字符串值
 * 语法：GET key
 *
 * @author Mini Redis
 */
public class GetCommand implements Command {

    private static final String NAME = "GET";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.size() != 1) {
            return RespMessage.error("wrong number of arguments for 'get' command");
        }

        String key = args.get(0);
        RedisDatabase db = client.getCurrentDatabase();

        // 获取值
        RedisObject obj = db.get(key);

        // 键不存在
        if (obj == null) {
            return RespMessage.nullBulkString();
        }

        // 检查类型
        if (obj.getType() != RedisDataType.STRING) {
            return RespMessage.error("WRONGTYPE Operation against a key holding the wrong kind of value");
        }

        // 返回字符串值
        String value = (String) obj.getValue();
        return new RespMessage.BulkString(value);
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
    public int getMinArgs() {
        return 1;
    }

    @Override
    public int getMaxArgs() {
        return 1;
    }

    @Override
    public String getDescription() {
        return "获取键的字符串值";
    }

    @Override
    public String getUsage() {
        return "GET key";
    }
}