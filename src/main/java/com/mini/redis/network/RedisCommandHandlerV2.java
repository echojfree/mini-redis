package com.mini.redis.network;

import com.mini.redis.command.Command;
import com.mini.redis.command.CommandFactory;
import com.mini.redis.config.RedisConfig;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.transaction.TransactionManager;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis command handler with transaction support
 *
 * Handles command execution and transaction queuing
 *
 * @author Mini Redis
 */
@ChannelHandler.Sharable
public class RedisCommandHandlerV2 extends SimpleChannelInboundHandler<RespMessage> {

    private static final Logger logger = LoggerFactory.getLogger(RedisCommandHandlerV2.class);
    private final RedisConfig config;
    private final CommandFactory commandFactory;

    public RedisCommandHandlerV2(RedisConfig config) {
        this.config = config;
        this.commandFactory = CommandFactory.getInstance();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // Create client instance
        RedisClient client = new RedisClient(ctx.channel());

        String clientInfo = ctx.channel().remoteAddress() != null
            ? ctx.channel().remoteAddress().toString()
            : "unknown";
        int count = RedisClient.getClientCount();

        logger.info("Client connected: {} (Current connections: {})", clientInfo, count);

        // Check max clients limit
        int maxClients = config.getMaxClients();
        if (maxClients > 0 && count > maxClients) {
            logger.warn("Max clients limit exceeded: {}", maxClients);
            ctx.writeAndFlush(new RespMessage.Error("max number of clients reached"));
            ctx.close();
            return;
        }

        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        RedisClient client = RedisClient.getClient(ctx.channel());
        if (client != null) {
            // Clean up transaction state
            TransactionManager.getInstance().removeContext(client);

            client.disconnect();
            int count = RedisClient.getClientCount();
            logger.info("Client disconnected: {} (Current connections: {})",
                client.getAddress(), count);
        }
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RespMessage msg) throws Exception {
        logger.debug("Received message: {}", msg);

        RedisClient client = RedisClient.getClient(ctx.channel());
        if (client == null) {
            logger.error("Client not found for channel");
            ctx.writeAndFlush(new RespMessage.Error("ERR internal error"));
            return;
        }

        try {
            // Update client activity
            client.updateActiveTime();

            // Commands are usually sent as arrays
            if (!(msg instanceof RespMessage.Array)) {
                logger.warn("Received non-array command: {}", msg);
                ctx.writeAndFlush(new RespMessage.Error("ERR invalid command format"));
                return;
            }

            RespMessage.Array commandArray = (RespMessage.Array) msg;
            if (commandArray.isEmpty()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR empty command"));
                return;
            }

            // Extract command name
            RespMessage firstElement = commandArray.getElements().get(0);
            if (!(firstElement instanceof RespMessage.BulkString)) {
                ctx.writeAndFlush(new RespMessage.Error("ERR invalid command format"));
                return;
            }

            String commandName = ((RespMessage.BulkString) firstElement)
                .getStringValue().toUpperCase();

            logger.info("Executing command: {}", commandName);

            // Check if client is in transaction
            if (client.isInTransaction() &&
                !isTransactionCommand(commandName)) {
                // Queue command for later execution
                TransactionManager.getInstance().enqueue(client, msg);
                ctx.writeAndFlush(new RespMessage.SimpleString("QUEUED"));
                return;
            }

            // Find and execute command
            Command command = commandFactory.getCommand(commandName);
            if (command == null) {
                ctx.writeAndFlush(new RespMessage.Error("ERR unknown command '" + commandName + "'"));
                return;
            }

            // Execute the command
            command.execute(ctx, client, msg);

        } catch (Exception e) {
            logger.error("Error processing command", e);
            ctx.writeAndFlush(new RespMessage.Error("ERR internal error: " + e.getMessage()));
        }
    }

    /**
     * Check if command is a transaction control command
     */
    private boolean isTransactionCommand(String commandName) {
        return "MULTI".equals(commandName) ||
               "EXEC".equals(commandName) ||
               "DISCARD".equals(commandName) ||
               "WATCH".equals(commandName) ||
               "UNWATCH".equals(commandName);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        RedisClient client = RedisClient.getClient(ctx.channel());
        String clientInfo = client != null ? client.getAddress() : "unknown";
        logger.error("Command handler exception: {}", clientInfo, cause);

        try {
            ctx.writeAndFlush(new RespMessage.Error("ERR server error"));
        } catch (Exception e) {
            logger.error("Failed to send error response", e);
        }

        ctx.close();
    }
}