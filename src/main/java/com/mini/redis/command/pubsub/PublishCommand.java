package com.mini.redis.command.pubsub;

import com.mini.redis.command.Command;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.pubsub.PubSubManager;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

/**
 * PUBLISH command - Publish message to channel
 *
 * @author Mini Redis
 */
public class PublishCommand implements Command {

    @Override
    public String getName() {
        return "PUBLISH";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            RespMessage.Array cmdArray = (RespMessage.Array) msg;
            List<RespMessage> args = cmdArray.getElements();

            if (args.size() != 3) {
                ctx.writeAndFlush(new RespMessage.Error("ERR wrong number of arguments for 'publish' command"));
                return;
            }

            String channel = ((RespMessage.BulkString) args.get(1)).getStringValue();
            String message = ((RespMessage.BulkString) args.get(2)).getStringValue();

            // Publish message
            int receivers = PubSubManager.getInstance().publish(channel, message);

            // Return number of receivers
            ctx.writeAndFlush(new RespMessage.Integer(receivers));

        } catch (Exception e) {
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }
}