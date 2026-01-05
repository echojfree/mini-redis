package com.mini.redis.network;

import com.mini.redis.protocol.RespMessage;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Redis 异常处理器
 * 处理管道中未捕获的异常和特殊事件
 *
 * 职责：
 * 1. 处理各种异常情况
 * 2. 处理空闲超时事件
 * 3. 记录错误日志
 * 4. 优雅关闭连接
 *
 * @author Mini Redis
 */
@ChannelHandler.Sharable
public class RedisExceptionHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(RedisExceptionHandler.class);

    /**
     * 处理用户事件（如空闲超时）
     *
     * @param ctx 通道上下文
     * @param evt 用户事件
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idleEvent = (IdleStateEvent) evt;
            logger.info("连接空闲超时: {} (状态: {})",
                ctx.channel().remoteAddress(), idleEvent.state());

            // 发送错误消息并关闭连接
            ctx.writeAndFlush(RespMessage.error("connection timeout"));
            ctx.close();
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    /**
     * 处理异常
     *
     * @param ctx 通道上下文
     * @param cause 异常原因
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String clientAddress = ctx.channel().remoteAddress().toString();

        // 根据异常类型进行不同处理
        if (cause instanceof IOException) {
            // IO 异常通常是客户端断开连接
            String message = cause.getMessage();
            if (message != null && (message.contains("Connection reset") ||
                    message.contains("Broken pipe"))) {
                logger.debug("客户端 {} 断开连接: {}", clientAddress, message);
            } else {
                logger.warn("IO 异常: {} - {}", clientAddress, message);
            }
        } else if (cause instanceof IllegalArgumentException) {
            // 参数异常，通常是协议错误
            logger.warn("协议错误: {} - {}", clientAddress, cause.getMessage());

            // 尝试发送错误响应
            try {
                ctx.writeAndFlush(RespMessage.error("protocol error: " + cause.getMessage()));
            } catch (Exception e) {
                logger.debug("发送错误响应失败", e);
            }
        } else {
            // 其他未预期的异常
            logger.error("未处理的异常: {}", clientAddress, cause);

            // 尝试发送通用错误响应
            try {
                ctx.writeAndFlush(RespMessage.error("internal server error"));
            } catch (Exception e) {
                logger.debug("发送错误响应失败", e);
            }
        }

        // 关闭连接
        ctx.close();
    }
}