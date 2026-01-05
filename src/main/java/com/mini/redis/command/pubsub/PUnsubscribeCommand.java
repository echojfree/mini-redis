package com.mini.redis.command.pubsub;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.pubsub.PubSubManager;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * PUNSUBSCRIBE command - Unsubscribe from patterns
 *
 * @author Mini Redis
 */
public class PUnsubscribeCommand implements Command {

    @Override
    public String getName() {
        return "PUNSUBSCRIBE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            RespMessage.Array cmdArray = (RespMessage.Array) msg;
            List<RespMessage> args = cmdArray.getElements();

            // Extract patterns (if any - no args means unsubscribe from all patterns)
            String[] patterns;
            if (args.size() > 1) {
                patterns = new String[args.size() - 1];
                for (int i = 1; i < args.size(); i++) {
                    patterns[i - 1] = ((RespMessage.BulkString) args.get(i)).getStringValue();
                }
            } else {
                // Unsubscribe from all patterns
                patterns = new String[0];
            }

            // Unsubscribe from patterns
            PubSubManager.getInstance().punsubscribe(client, patterns);

            // Response is sent by PubSubManager

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}