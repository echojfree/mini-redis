package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;

import java.util.List;
import java.util.Set;

/**
 * KEYS 命令实现
 *
 * 功能：查找所有符合模式的键
 * 语法：KEYS pattern
 *
 * 支持的模式：
 * - * 匹配任意字符
 * - ? 匹配单个字符（暂未实现）
 * - [...] 字符集合（暂未实现）
 *
 * 警告：生产环境慎用，可能阻塞服务器
 *
 * @author Mini Redis
 */
public class KeysCommand implements Command {

    private static final String NAME = "KEYS";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.size() != 1) {
            return RespMessage.error("wrong number of arguments for 'keys' command");
        }

        String pattern = args.get(0);
        RedisDatabase db = client.getCurrentDatabase();

        // 获取匹配的键
        Set<String> keys = db.keys(pattern);

        // 构造数组响应
        RespMessage.Array array = new RespMessage.Array();
        for (String key : keys) {
            array.addElement(new RespMessage.BulkString(key));
        }

        return array;
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
        return "查找所有符合模式的键";
    }

    @Override
    public String getUsage() {
        return "KEYS pattern";
    }
}