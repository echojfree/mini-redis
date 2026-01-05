package com.mini.redis.command.transaction;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.transaction.TransactionManager;
import io.netty.channel.ChannelHandlerContext;

/**
 * MULTI command implementation
 * Start a transaction block
 *
 * Syntax: MULTI
 * Return: Always OK
 *
 * Interview points:
 * 1. Transaction isolation
 * 2. Command queuing mechanism
 * 3. Client state management
 *
 * @author Mini Redis
 */
public class MultiCommand implements Command {

    @Override
    public String getName() {
        return "MULTI";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            // Check if already in transaction
            if (client.isInTransaction()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR MULTI calls can not be nested"));
                return;
            }

            // Start transaction
            TransactionManager.getInstance().multi(client);
            client.setInTransaction(true);

            // Return OK
            ctx.writeAndFlush(new RespMessage.SimpleString("OK"));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}