package com.mini.redis.command.persistence;

import com.mini.redis.command.Command;
import com.mini.redis.persistence.RdbPersistence;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandlerContext;

/**
 * SAVE command - Synchronously save to disk
 *
 * @author Mini Redis
 */
public class SaveCommand implements Command {

    @Override
    public String getName() {
        return "SAVE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            RdbPersistence rdb = RdbPersistence.getInstance();

            // Perform synchronous save
            boolean success = rdb.save();

            if (success) {
                ctx.writeAndFlush(new RespMessage.SimpleString("OK"));
            } else {
                ctx.writeAndFlush(new RespMessage.Error("ERR save failed"));
            }

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}