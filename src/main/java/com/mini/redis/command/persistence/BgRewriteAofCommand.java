package com.mini.redis.command.persistence;

import com.mini.redis.command.Command;
import com.mini.redis.persistence.AofPersistence;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandlerContext;

/**
 * BGREWRITEAOF command - Background AOF rewrite
 *
 * @author Mini Redis
 */
public class BgRewriteAofCommand implements Command {

    @Override
    public String getName() {
        return "BGREWRITEAOF";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            AofPersistence aof = AofPersistence.getInstance();

            if (!aof.isEnabled()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR AOF is not enabled"));
                return;
            }

            if (aof.isRewriteInProgress()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR background AOF rewrite already in progress"));
                return;
            }

            // Start background rewrite
            boolean started = aof.rewriteAof();

            if (started) {
                ctx.writeAndFlush(new RespMessage.SimpleString("Background AOF rewrite started"));
            } else {
                ctx.writeAndFlush(new RespMessage.Error("ERR background AOF rewrite failed to start"));
            }

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}