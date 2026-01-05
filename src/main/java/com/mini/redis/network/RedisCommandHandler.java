package com.mini.redis.network;

import com.mini.redis.command.Command;
import com.mini.redis.command.CommandFactory;
import com.mini.redis.config.RedisConfig;
import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis 命令处理器
 * 负责接收解码后的 RESP 消息，执行对应的命令，并返回结果
 *
 * 处理流程：
 * 1. 接收客户端发送的命令（通常是 Array 类型）
 * 2. 解析命令名和参数
 * 3. 查找并执行对应的命令处理器
 * 4. 将执行结果编码并发送给客户端
 *
 * @author Mini Redis
 */
@ChannelHandler.Sharable
public class RedisCommandHandler extends SimpleChannelInboundHandler<RespMessage> {

    private static final Logger logger = LoggerFactory.getLogger(RedisCommandHandler.class);

    /**
     * 配置管理器
     */
    private final RedisConfig config;

    /**
     * 命令工厂
     */
    private final CommandFactory commandFactory;

    /**
     * 当前客户端实例
     */
    private RedisClient client;

    /**
     * 构造函数
     *
     * @param config 配置管理器
     */
    public RedisCommandHandler(RedisConfig config) {
        this.config = config;
        this.commandFactory = CommandFactory.getInstance();
    }

    /**
     * 客户端连接建立时调用
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        // 创建客户端实例
        client = new RedisClient(ctx.channel());

        String clientInfo = ctx.channel().remoteAddress().toString();
        int count = RedisClient.getClientCount();

        logger.info("客户端连接: {} (当前连接数: {})", clientInfo, count);

        // 检查最大连接数限制
        int maxClients = config.getMaxClients();
        if (maxClients > 0 && count > maxClients) {
            logger.warn("超过最大连接数限制: {}", maxClients);
            ctx.writeAndFlush(RespMessage.error("max number of clients reached"));
            ctx.close();
            return;
        }

        super.channelActive(ctx);
    }

    /**
     * 客户端断开连接时调用
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (client != null) {
            client.disconnect();
            int count = RedisClient.getClientCount();
            logger.info("客户端断开: {} (当前连接数: {})", client.getAddress(), count);
            client = null;
        }
        super.channelInactive(ctx);
    }

    /**
     * 处理接收到的消息
     *
     * @param ctx 通道上下文
     * @param msg 接收到的 RESP 消息
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RespMessage msg) throws Exception {
        logger.debug("收到消息: {}", msg);

        try {
            // Redis 命令通常以数组形式发送
            if (!(msg instanceof RespMessage.Array)) {
                // 某些客户端可能发送单个命令（如 PING）
                if (msg instanceof RespMessage.BulkString) {
                    RespMessage response = handleSingleCommand(
                        ((RespMessage.BulkString) msg).getStringValue());
                    ctx.writeAndFlush(response);
                    return;
                }

                logger.warn("收到非数组格式的命令: {}", msg);
                ctx.writeAndFlush(RespMessage.error("invalid command format"));
                return;
            }

            // 解析命令数组
            RespMessage.Array commandArray = (RespMessage.Array) msg;
            if (commandArray.isEmpty()) {
                ctx.writeAndFlush(RespMessage.error("empty command"));
                return;
            }

            // 提取命令名和参数
            List<String> commandParts = parseCommandArray(commandArray);
            if (commandParts.isEmpty()) {
                ctx.writeAndFlush(RespMessage.error("invalid command"));
                return;
            }

            String commandName = commandParts.get(0).toUpperCase();
            List<String> args = commandParts.subList(1, commandParts.size());

            logger.info("执行命令: {} {}", commandName, args);

            // 执行命令
            RespMessage response = executeCommand(commandName, args, ctx);

            // 发送响应
            ctx.writeAndFlush(response);

        } catch (Exception e) {
            logger.error("处理命令时发生错误", e);
            ctx.writeAndFlush(RespMessage.error("internal error: " + e.getMessage()));
        }
    }

    /**
     * 解析命令数组
     *
     * @param array 命令数组
     * @return 命令部分列表（命令名 + 参数）
     */
    private List<String> parseCommandArray(RespMessage.Array array) {
        List<String> parts = new ArrayList<>();

        for (RespMessage element : array.getElements()) {
            if (element instanceof RespMessage.BulkString) {
                RespMessage.BulkString bulkString = (RespMessage.BulkString) element;
                if (!bulkString.isNull()) {
                    parts.add(bulkString.getStringValue());
                }
            } else if (element instanceof RespMessage.SimpleString) {
                parts.add(((RespMessage.SimpleString) element).getValue());
            }
        }

        return parts;
    }

    /**
     * 处理单个命令（非数组格式）
     *
     * @param command 命令字符串
     * @return 响应消息
     */
    private RespMessage handleSingleCommand(String command) {
        if (command == null || command.isEmpty()) {
            return RespMessage.error("empty command");
        }

        String upperCommand = command.toUpperCase();
        if ("PING".equals(upperCommand)) {
            return RespMessage.pong();
        }

        return RespMessage.error("unknown command '" + command + "'");
    }

    /**
     * 执行命令
     *
     * @param commandName 命令名
     * @param args 参数列表
     * @param ctx 通道上下文
     * @return 响应消息
     */
    private RespMessage executeCommand(String commandName, List<String> args,
                                       ChannelHandlerContext ctx) {
        try {
            // 更新客户端活动时间
            if (client != null) {
                client.updateActiveTime();
            }

            // 查找命令处理器
            Command command = commandFactory.getCommand(commandName);
            if (command == null) {
                return RespMessage.error("unknown command '" + commandName + "'");
            }

            // 验证参数数量
            if (!command.validateArgCount(args.size())) {
                return RespMessage.error("wrong number of arguments for '" +
                    commandName.toLowerCase() + "' command");
            }

            // 执行命令
            return command.execute(args, client);

        } catch (IllegalArgumentException e) {
            // 参数错误
            return RespMessage.error(e.getMessage());
        } catch (Exception e) {
            // 其他错误
            logger.error("执行命令 {} 时发生错误", commandName, e);
            return RespMessage.error("internal error");
        }
    }

    /**
     * 异常处理
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String clientInfo = client != null ? client.getAddress() : "unknown";
        logger.error("命令处理器异常: {}", clientInfo, cause);

        // 发送错误响应
        try {
            ctx.writeAndFlush(RespMessage.error("server error"));
        } catch (Exception e) {
            logger.error("发送错误响应失败", e);
        }

        // 关闭连接
        ctx.close();
    }
}