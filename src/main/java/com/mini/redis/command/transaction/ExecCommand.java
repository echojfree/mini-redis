package com.mini.redis.command.transaction;

import com.mini.redis.command.Command;
import com.mini.redis.command.CommandFactory;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import com.mini.redis.transaction.TransactionManager;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * EXEC command implementation
 * Execute all commands in a transaction
 *
 * Syntax: EXEC
 * Return: Array of command results or nil if transaction was aborted
 *
 * Interview points:
 * 1. Atomic execution (all or nothing)
 * 2. Optimistic locking with WATCH
 * 3. Result aggregation
 * 4. Rollback mechanism (Redis doesn't support rollback)
 *
 * @author Mini Redis
 */
public class ExecCommand implements Command {

    @Override
    public String getName() {
        return "EXEC";
    }

    @Override
    public void execute(ChannelHandlerContext ctx, RedisClient client, RespMessage msg) {
        try {
            // Check if in transaction
            if (!client.isInTransaction()) {
                ctx.writeAndFlush(new RespMessage.Error("ERR EXEC without MULTI"));
                return;
            }

            // Execute transaction
            List<TransactionManager.QueuedCommand> commands =
                TransactionManager.getInstance().exec(client);

            // Clear transaction flag
            client.setInTransaction(false);

            // Check if transaction was aborted (WATCH key was modified)
            if (commands == null) {
                // Return nil for aborted transaction
                ctx.writeAndFlush(new RespMessage.BulkString(null));
                return;
            }

            // Execute all queued commands
            List<RespMessage> results = new ArrayList<>();

            for (TransactionManager.QueuedCommand queuedCmd : commands) {
                try {
                    // Parse the command
                    RespMessage.Array cmdArray = (RespMessage.Array) queuedCmd.getCommand();
                    if (cmdArray.getElements().isEmpty()) {
                        results.add(new RespMessage.Error("ERR empty command"));
                        continue;
                    }

                    // Get command name
                    String cmdName = ((RespMessage.BulkString) cmdArray.getElements().get(0))
                        .getStringValue().toUpperCase();

                    // Get command handler
                    Command command = CommandFactory.getInstance().getCommand(cmdName);
                    if (command == null) {
                        results.add(new RespMessage.Error("ERR unknown command '" + cmdName + "'"));
                        continue;
                    }

                    // Create a capturing context for the result
                    CapturingContext capturingContext = new CapturingContext(ctx);

                    // Execute the command
                    command.execute(capturingContext, client, queuedCmd.getCommand());

                    // Get the captured result
                    RespMessage result = capturingContext.getCapturedMessage();
                    if (result != null) {
                        results.add(result);
                    } else {
                        results.add(new RespMessage.SimpleString("OK"));
                    }

                } catch (Exception e) {
                    // Add error to results but continue execution
                    results.add(new RespMessage.Error("ERR " + e.getMessage()));
                }
            }

            // Return array of results
            ctx.writeAndFlush(new RespMessage.Array(results));

        } catch (Exception e) {
            client.setInTransaction(false);
            ctx.writeAndFlush(new RespMessage.Error("ERR " + e.getMessage()));
        }
    }

    /**
     * Context that captures command results instead of sending them
     */
    private static class CapturingContext extends ChannelHandlerContext {
        private final ChannelHandlerContext originalContext;
        private RespMessage capturedMessage;

        public CapturingContext(ChannelHandlerContext originalContext) {
            this.originalContext = originalContext;
        }

        @Override
        public ChannelHandlerContext writeAndFlush(Object msg) {
            if (msg instanceof RespMessage) {
                this.capturedMessage = (RespMessage) msg;
            }
            return this;
        }

        public RespMessage getCapturedMessage() {
            return capturedMessage;
        }

        // Delegate all other methods to original context
        @Override
        public io.netty.channel.Channel channel() {
            return originalContext.channel();
        }

        @Override
        public io.netty.util.concurrent.EventExecutor executor() {
            return originalContext.executor();
        }

        @Override
        public String name() {
            return originalContext.name();
        }

        @Override
        public io.netty.channel.ChannelHandler handler() {
            return originalContext.handler();
        }

        @Override
        public boolean isRemoved() {
            return originalContext.isRemoved();
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireChannelRegistered() {
            return originalContext.fireChannelRegistered();
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireChannelUnregistered() {
            return originalContext.fireChannelUnregistered();
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireChannelActive() {
            return originalContext.fireChannelActive();
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireChannelInactive() {
            return originalContext.fireChannelInactive();
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireExceptionCaught(Throwable cause) {
            return originalContext.fireExceptionCaught(cause);
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireUserEventTriggered(Object evt) {
            return originalContext.fireUserEventTriggered(evt);
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireChannelRead(Object msg) {
            return originalContext.fireChannelRead(msg);
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireChannelReadComplete() {
            return originalContext.fireChannelReadComplete();
        }

        @Override
        public io.netty.channel.ChannelHandlerContext fireChannelWritabilityChanged() {
            return originalContext.fireChannelWritabilityChanged();
        }

        @Override
        public io.netty.channel.ChannelHandlerContext flush() {
            return originalContext.flush();
        }

        @Override
        public io.netty.channel.ChannelPipeline pipeline() {
            return originalContext.pipeline();
        }

        @Override
        public io.netty.buffer.ByteBufAllocator alloc() {
            return originalContext.alloc();
        }

        @Override
        public <T> io.netty.util.Attribute<T> attr(io.netty.util.AttributeKey<T> key) {
            return originalContext.attr(key);
        }

        @Override
        public <T> boolean hasAttr(io.netty.util.AttributeKey<T> key) {
            return originalContext.hasAttr(key);
        }

        @Override
        public io.netty.channel.ChannelFuture bind(java.net.SocketAddress localAddress) {
            return originalContext.bind(localAddress);
        }

        @Override
        public io.netty.channel.ChannelFuture connect(java.net.SocketAddress remoteAddress) {
            return originalContext.connect(remoteAddress);
        }

        @Override
        public io.netty.channel.ChannelFuture connect(java.net.SocketAddress remoteAddress,
                                                       java.net.SocketAddress localAddress) {
            return originalContext.connect(remoteAddress, localAddress);
        }

        @Override
        public io.netty.channel.ChannelFuture disconnect() {
            return originalContext.disconnect();
        }

        @Override
        public io.netty.channel.ChannelFuture close() {
            return originalContext.close();
        }

        @Override
        public io.netty.channel.ChannelFuture deregister() {
            return originalContext.deregister();
        }

        @Override
        public io.netty.channel.ChannelFuture bind(java.net.SocketAddress localAddress,
                                                   io.netty.channel.ChannelPromise promise) {
            return originalContext.bind(localAddress, promise);
        }

        @Override
        public io.netty.channel.ChannelFuture connect(java.net.SocketAddress remoteAddress,
                                                      io.netty.channel.ChannelPromise promise) {
            return originalContext.connect(remoteAddress, promise);
        }

        @Override
        public io.netty.channel.ChannelFuture connect(java.net.SocketAddress remoteAddress,
                                                      java.net.SocketAddress localAddress,
                                                      io.netty.channel.ChannelPromise promise) {
            return originalContext.connect(remoteAddress, localAddress, promise);
        }

        @Override
        public io.netty.channel.ChannelFuture disconnect(io.netty.channel.ChannelPromise promise) {
            return originalContext.disconnect(promise);
        }

        @Override
        public io.netty.channel.ChannelFuture close(io.netty.channel.ChannelPromise promise) {
            return originalContext.close(promise);
        }

        @Override
        public io.netty.channel.ChannelFuture deregister(io.netty.channel.ChannelPromise promise) {
            return originalContext.deregister(promise);
        }

        @Override
        public io.netty.channel.ChannelHandlerContext read() {
            return originalContext.read();
        }

        @Override
        public io.netty.channel.ChannelFuture write(Object msg) {
            if (msg instanceof RespMessage) {
                this.capturedMessage = (RespMessage) msg;
                return null;
            }
            return originalContext.write(msg);
        }

        @Override
        public io.netty.channel.ChannelFuture write(Object msg, io.netty.channel.ChannelPromise promise) {
            if (msg instanceof RespMessage) {
                this.capturedMessage = (RespMessage) msg;
                return null;
            }
            return originalContext.write(msg, promise);
        }

        @Override
        public io.netty.channel.ChannelFuture writeAndFlush(Object msg, io.netty.channel.ChannelPromise promise) {
            if (msg instanceof RespMessage) {
                this.capturedMessage = (RespMessage) msg;
                return null;
            }
            return originalContext.writeAndFlush(msg, promise);
        }

        @Override
        public io.netty.channel.ChannelPromise newPromise() {
            return originalContext.newPromise();
        }

        @Override
        public io.netty.channel.ChannelProgressivePromise newProgressivePromise() {
            return originalContext.newProgressivePromise();
        }

        @Override
        public io.netty.channel.ChannelFuture newSucceededFuture() {
            return originalContext.newSucceededFuture();
        }

        @Override
        public io.netty.channel.ChannelFuture newFailedFuture(Throwable cause) {
            return originalContext.newFailedFuture(cause);
        }

        @Override
        public io.netty.channel.ChannelPromise voidPromise() {
            return originalContext.voidPromise();
        }
    }
}