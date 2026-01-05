package com.mini.redis.command.transaction;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.transaction.TransactionManager;
import io.netty.channel.ChannelHandlerContext;

/**
 * DISCARD command implementation
 * Discard all commands in a transaction
 *
 * Syntax: DISCARD
 * Return: Always OK
 *
 * Interview points:
 * 1. Transaction cancellation
 * 2. Resource cleanup
 * 3. State management
 *
 * @author Mini Redis
 */
public class DiscardCommand implements Command {

    @Override
    public String getName() {
        return "DISCARD";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            // Check if in transaction
            if (!client.isInTransaction()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR DISCARD without MULTI"));
                return;
            }

            // Discard transaction
            TransactionManager.getInstance().discard(client);
            client.setInTransaction(false);

            // Return OK
            ctx.writeAndFlush(new RespMessage.SimpleString("OK"));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}