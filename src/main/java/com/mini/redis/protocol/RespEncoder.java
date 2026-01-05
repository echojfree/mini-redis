package com.mini.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RESP 协议编码器
 * 将 RESP 消息对象编码为字节流
 *
 * 编码流程：
 * 1. 接收 RespMessage 对象
 * 2. 调用对象的 encode 方法
 * 3. 将编码后的字节写入输出缓冲区
 *
 * @author Mini Redis
 */
public class RespEncoder extends MessageToByteEncoder<RespMessage> {

    private static final Logger logger = LoggerFactory.getLogger(RespEncoder.class);

    /**
     * 编码方法
     * Netty 会在发送消息时调用此方法
     *
     * @param ctx 通道上下文
     * @param msg 要编码的消息
     * @param out 输出缓冲区
     * @throws Exception 编码异常
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, RespMessage msg, ByteBuf out) throws Exception {
        if (msg == null) {
            logger.warn("尝试编码 null 消息");
            return;
        }

        try {
            // 记录编码前的写入位置
            int beforeSize = out.writerIndex();

            // 调用消息对象的编码方法
            msg.encode(out);

            // 计算编码后的字节数
            int encodedSize = out.writerIndex() - beforeSize;
            logger.debug("编码消息: {} ({}字节)", msg, encodedSize);

        } catch (Exception e) {
            logger.error("编码消息失败: {}", msg, e);
            throw e;
        }
    }

    /**
     * 异常处理
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("编码器异常: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}