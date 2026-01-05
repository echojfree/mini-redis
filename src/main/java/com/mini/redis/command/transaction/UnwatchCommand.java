package com.mini.redis.command.transaction;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.transaction.TransactionManager;
import io.netty.channel.ChannelHandlerContext;

/**
 * UNWATCH command implementation
 * Forget about all watched keys
 *
 * Syntax: UNWATCH
 * Return: Always OK
 *
 * Interview points:
 * 1. Cleanup of watched keys
 * 2. Resource management
 * 3. State reset
 *
 * @author Mini Redis
 */
public class UnwatchCommand implements Command {

    @Override
    public String getName() {
        return "UNWATCH";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            // Cannot UNWATCH inside a transaction
            if (client.isInTransaction()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR UNWATCH inside MULTI is not allowed"));
                return;
            }

            // Unwatch all keys
            TransactionManager.getInstance().unwatch(client);

            // Return OK
            ctx.writeAndFlush(new RespMessage.SimpleString("OK"));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}