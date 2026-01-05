package com.mini.redis.command.impl;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * PING 命令实现
 *
 * 功能：测试服务器连接状态
 * 语法：PING [message]
 * 返回：
 * - 无参数时返回 PONG
 * - 有参数时返回该参数
 *
 * 示例：
 * > PING
 * < PONG
 * > PING "Hello World"
 * < "Hello World"
 *
 * @author Mini Redis
 */
public class PingCommand implements Command {

    /**
     * 命令名称
     */
    private static final String NAME = "PING";

    @Override
    public RespMessage execute(List<String> args, ChannelHandlerContext ctx) {
        // 无参数时返回 PONG
        if (args.isEmpty()) {
            return RespMessage.pong();
        }

        // 有参数时返回第一个参数
        String message = args.get(0);
        return new RespMessage.BulkString(message);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean validateArgCount(int argCount) {
        // PING 命令可以有 0 个或 1 个参数
        return argCount >= 0 && argCount <= 1;
    }

    @Override
    public int getMinArgs() {
        return 0;
    }

    @Override
    public int getMaxArgs() {
        return 1;
    }

    @Override
    public String getDescription() {
        return "测试服务器连接状态";
    }

    @Override
    public String getUsage() {
        return "PING [message]";
    }
}