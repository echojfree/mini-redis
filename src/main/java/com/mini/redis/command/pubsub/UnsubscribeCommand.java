package com.mini.redis.command.pubsub;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.pubsub.PubSubManager;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * UNSUBSCRIBE command - Unsubscribe from channels
 *
 * @author Mini Redis
 */
public class UnsubscribeCommand implements Command {

    @Override
    public String getName() {
        return "UNSUBSCRIBE";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            RespMessage.Array cmdArray = (RespMessage.Array) msg;
            List<RespMessage> args = cmdArray.getElements();

            // Extract channels (if any - no args means unsubscribe from all)
            String[] channels;
            if (args.size() > 1) {
                channels = new String[args.size() - 1];
                for (int i = 1; i < args.size(); i++) {
                    channels[i - 1] = ((RespMessage.BulkString) args.get(i)).getStringValue();
                }
            } else {
                // Unsubscribe from all channels
                channels = new String[0];
            }

            // Unsubscribe from channels
            PubSubManager.getInstance().unsubscribe(client, channels);

            // Response is sent by PubSubManager

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}