package com.mini.redis.network;

import com.mini.redis.config.RedisConfig;
import com.mini.redis.protocol.RespDecoder;
import com.mini.redis.protocol.RespEncoder;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Redis 通道初始化器
 * 负责配置客户端连接的处理管道（Pipeline）
 *
 * Pipeline 处理链：
 * 1. IdleStateHandler - 空闲检测，用于心跳和超时处理
 * 2. RespDecoder - RESP 协议解码器
 * 3. RespEncoder - RESP 协议编码器
 * 4. RedisCommandHandler - 命令处理器
 *
 * @author Mini Redis
 */
public class RedisChannelInitializer extends ChannelInitializer<SocketChannel> {

    private static final Logger logger = LoggerFactory.getLogger(RedisChannelInitializer.class);

    /**
     * 配置管理器
     */
    private final RedisConfig config;

    /**
     * 最大帧长度（512MB）
     * Redis 协议中最大的 bulk string 可以达到 512MB
     */
    private static final int MAX_FRAME_LENGTH = 512 * 1024 * 1024;

    /**
     * 构造函数
     *
     * @param config 配置管理器
     */
    public RedisChannelInitializer(RedisConfig config) {
        this.config = config;
    }

    /**
     * 初始化通道
     * 当新的客户端连接建立时，此方法会被调用
     *
     * @param ch 客户端套接字通道
     * @throws Exception 如果初始化失败
     */
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        logger.debug("初始化新的客户端连接: {}", ch.remoteAddress());

        // 获取通道管道
        ChannelPipeline pipeline = ch.pipeline();

        // 1. 添加空闲状态处理器
        // 用于检测连接空闲状态，实现心跳和超时机制
        int timeout = config.getInt("server.timeout", 0);
        if (timeout > 0) {
            pipeline.addLast("idleStateHandler",
                new IdleStateHandler(timeout, 0, 0, TimeUnit.SECONDS));
        }

        // 2. 添加 RESP 协议解码器
        // 将字节流解码为 RESP 协议对象
        pipeline.addLast("respDecoder", new RespDecoder());

        // 3. 添加 RESP 协议编码器
        // 将 RESP 协议对象编码为字节流
        pipeline.addLast("respEncoder", new RespEncoder());

        // 4. 添加命令处理器
        // 处理解码后的 Redis 命令
        pipeline.addLast("commandHandler", new RedisCommandHandler(config));

        // 5. 添加异常处理器
        // 处理管道中未捕获的异常
        pipeline.addLast("exceptionHandler", new RedisExceptionHandler());

        logger.debug("客户端连接初始化完成: {}", ch.remoteAddress());
    }

    /**
     * 处理通道初始化异常
     *
     * @param ctx 通道上下文
     * @param cause 异常原因
     * @throws Exception 如果处理失败
     */
    @Override
    public void exceptionCaught(io.netty.channel.ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        logger.error("通道初始化异常: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}