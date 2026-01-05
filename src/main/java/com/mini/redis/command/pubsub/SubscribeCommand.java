package com.mini.redis.command.pubsub;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.pubsub.PubSubManager;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * SUBSCRIBE command - Subscribe to channels
 *
 * @author Mini Redis
 */
public class SubscribeCommand implements Command {

    @Override
    public String getName() {
        return "SUBSCRIBE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            RespMessage.Array cmdArray = (RespMessage.Array) msg;
            List<RespMessage> args = cmdArray.getElements();

            if (args.size() < 2) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'subscribe' command"));
                return;
            }

            // Extract channels
            String[] channels = new String[args.size() - 1];
            for (int i = 1; i < args.size(); i++) {
                channels[i - 1] = ((RespMessage.BulkString) args.get(i)).getStringValue();
            }

            // Subscribe to channels
            PubSubManager.getInstance().subscribe(client, channels);

            // Response is sent by PubSubManager

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}