package com.mini.redis.command.persistence;

import com.mini.redis.command.Command;
import com.mini.redis.persistence.RdbPersistence;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandlerContext;

/**
 * BGSAVE command - Background save to disk
 *
 * @author Mini Redis
 */
public class BgSaveCommand implements Command {

    @Override
    public String getName() {
        return "BGSAVE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            RdbPersistence rdb = RdbPersistence.getInstance();

            if (rdb.isBgSaveInProgress()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR background save already in progress"));
                return;
            }

            // Start background save
            boolean started = rdb.backgroundSave();

            if (started) {
                ctx.writeAndFlush(new RespMessage.SimpleString("Background saving started"));
            } else {
                ctx.writeAndFlush(new RespMessage.Error("ERR background save failed to start"));
            }

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}