package com.mini.redis.command.transaction;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.transaction.TransactionManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * WATCH command implementation
 * Watch keys for modifications (optimistic locking)
 *
 * Syntax: WATCH key [key ...]
 * Return: Always OK
 *
 * Interview points:
 * 1. Optimistic locking mechanism
 * 2. CAS (Compare And Swap) operations
 * 3. Version control for keys
 * 4. Transaction abort conditions
 *
 * @author Mini Redis
 */
public class WatchCommand implements Command {

    @Override
    public String getName() {
        return "WATCH";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            RespMessage.Array cmdArray = (RespMessage.Array) msg;
            List<RespMessage> args = cmdArray.getElements();

            // Check arguments
            if (args.size() < 2) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'watch' command"));
                return;
            }

            // Cannot WATCH inside a transaction
            if (client.isInTransaction()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR WATCH inside MULTI is not allowed"));
                return;
            }

            // Watch all specified keys
            TransactionManager tm = TransactionManager.getInstance();
            for (int i = 1; i < args.size(); i++) {
                String key = ((RespMessage.BulkString) args.get(i)).getStringValue();
                tm.watch(client, key);
            }

            // Return OK
            ctx.writeAndFlush(new RespMessage.SimpleString("OK"));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}