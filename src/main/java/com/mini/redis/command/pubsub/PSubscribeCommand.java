package com.mini.redis.command.pubsub;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.pubsub.PubSubManager;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * PSUBSCRIBE command - Subscribe to patterns
 *
 * @author Mini Redis
 */
public class PSubscribeCommand implements Command {

    @Override
    public String getName() {
        return "PSUBSCRIBE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            RespMessage.Array cmdArray = (RespMessage.Array) msg;
            List<RespMessage> args = cmdArray.getElements();

            if (args.size() < 2) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'psubscribe' command"));
                return;
            }

            // Extract patterns
            String[] patterns = new String[args.size() - 1];
            for (int i = 1; i < args.size(); i++) {
                patterns[i - 1] = ((RespMessage.BulkString) args.get(i)).getStringValue();
            }

            // Subscribe to patterns
            PubSubManager.getInstance().psubscribe(client, patterns);

            // Response is sent by PubSubManager

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}