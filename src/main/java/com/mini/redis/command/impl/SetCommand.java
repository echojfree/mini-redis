package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.storage.RedisDatabase;
import com.mini.redis.storage.RedisDataType;
import com.mini.redis.storage.RedisObject;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * SET 命令实现
 *
 * 功能：设置键的字符串值
 * 语法：SET key value [NX|XX] [GET] [EX seconds|PX milliseconds|EXAT unix-time-seconds|PXAT unix-time-milliseconds|KEEPTTL]
 *
 * 简化版本只支持：SET key value [EX seconds]
 *
 * @author Mini Redis
 */
public class SetCommand implements Command {

    private static final String NAME = "SET";

    @Override
    public RespMessage execute(List<String> args, RedisClient client) {
        // 更新活动时间
        client.updateActiveTime();

        // 至少需要 key 和 value
        if (args.size() < 2) {
            return RespMessage.error("wrong number of arguments for 'set' command");
        }

        String key = args.get(0);
        String value = args.get(1);

        // 处理可选参数
        Long expireSeconds = null;
        boolean nx = false;  // 仅当键不存在时设置
        boolean xx = false;  // 仅当键存在时设置

        // 解析可选参数
        for (int i = 2; i < args.size(); i++) {
            String arg = args.get(i).toUpperCase();

            if ("NX".equals(arg)) {
                nx = true;
            } else if ("XX".equals(arg)) {
                xx = true;
            } else if ("EX".equals(arg)) {
                // 下一个参数是过期时间（秒）
                if (i + 1 < args.size()) {
                    try {
                        expireSeconds = Long.parseLong(args.get(++i));
                    } catch (NumberFormatException e) {
                        return RespMessage.error("value is not an integer or out of range");
                    }
                } else {
                    return RespMessage.error("syntax error");
                }
            }
        }

        // NX 和 XX 不能同时使用
        if (nx && xx) {
            return RespMessage.error("syntax error");
        }

        RedisDatabase db = client.getCurrentDatabase();

        // 检查 NX/XX 条件
        boolean exists = db.exists(key);
        if (nx && exists) {
            return RespMessage.nullBulkString();  // NX 模式下键已存在
        }
        if (xx && !exists) {
            return RespMessage.nullBulkString();  // XX 模式下键不存在
        }

        // 创建字符串对象
        RedisObject obj = new RedisObject(RedisDataType.STRING, value);

        // 设置过期时间
        if (expireSeconds != null) {
            obj.setExpire(expireSeconds, TimeUnit.SECONDS);
        }

        // 存储到数据库
        db.set(key, obj);

        return RespMessage.ok();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean validateArgCount(int argCount) {
        return argCount >= 2;  // 至少需要 key 和 value
    }

    @Override
    public int getMinArgs() {
        return 2;
    }

    @Override
    public int getMaxArgs() {
        return -1;  // 支持可选参数
    }

    @Override
    public String getDescription() {
        return "设置键的字符串值";
    }

    @Override
    public String getUsage() {
        return "SET key value [EX seconds] [NX|XX]";
    }
}