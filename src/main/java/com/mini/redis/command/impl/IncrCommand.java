package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;

import java.util.List;

/**
 * INCR 命令实现
 *
 * 功能：将键的整数值加一
 * 语法：INCR key
 *
 * 如果键不存在，先初始化为 0 再执行加一操作
 *
 * @author Mini Redis
 */
public class IncrCommand implements Command {

    private static final String NAME = "INCR";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        client.updateActiveTime();

        if (args.size() != 1) {
            return RespMessage.error("wrong number of arguments for 'incr' command");
        }

        String key = args.get(0);
        RedisDatabase db = client.getCurrentDatabase();

        // 获取现有值
        RedisObject obj = db.get(key);

        long newValue;

        if (obj == null) {
            // 键不存在，初始化为 1
            newValue = 1;
        } else {
            // 检查类型
            if (obj.getType() != RedisDataType.STRING) {
                return RespMessage.error("WRONGTYPE Operation against a key holding the wrong kind of value");
            }

            // 获取当前值
            String currentValue = (String) obj.getValue();

            // 尝试转换为整数
            try {
                long value = Long.parseLong(currentValue);

                // 检查溢出
                if (value == Long.MAX_VALUE) {
                    return RespMessage.error("increment or decrement would overflow");
                }

                newValue = value + 1;
            } catch (NumberFormatException e) {
                return RespMessage.error("value is not an integer or out of range");
            }
        }

        // 设置新值
        RedisObject newObj = new RedisObject(RedisDataType.STRING, String.valueOf(newValue));
        db.set(key, newObj);

        // 返回新值
        return new RespMessage.Integer(newValue);
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
        return "将键的整数值加一";
    }

    @Override
    public String getUsage() {
        return "INCR key";
    }
}