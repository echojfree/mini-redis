package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.DatabaseManager;

import java.util.List;

/**
 * SELECT 命令实现
 *
 * 功能：切换到指定的数据库
 * 语法：SELECT index
 *
 * Redis 默认有 16 个数据库（0-15）
 *
 * @author Mini Redis
 */
public class SelectCommand implements Command {

    private static final String NAME = "SELECT";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.size() != 1) {
            return RespMessage.error("wrong number of arguments for 'select' command");
        }

        // 解析数据库索引
        int index;
        try {
            index = Integer.parseInt(args.get(0));
        } catch (NumberFormatException e) {
            return RespMessage.error("invalid DB index");
        }

        // 检查索引范围
        if (index < 0 || index >= DatabaseManager.getInstance().getDatabaseCount()) {
            return RespMessage.error("invalid DB index");
        }

        // 切换数据库
        client.selectDatabase(index);

        return RespMessage.ok();
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
        return "切换到指定的数据库";
    }

    @Override
    public String getUsage() {
        return "SELECT index";
    }
}