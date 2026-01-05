package com.mini.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RESP 协议解码器
 * 将字节流解码为 RESP 消息对象
 *
 * 解码流程：
 * 1. 读取类型前缀（+、-、:、$、*）
 * 2. 根据类型解析对应的数据格式
 * 3. 构造相应的 RespMessage 对象
 *
 * 面试知识点：
 * - Netty 解码器原理
 * - 有状态的解码处理
 * - 粘包/拆包问题解决
 *
 * @author Mini Redis
 */
public class RespDecoder extends ByteToMessageDecoder {

    private static final Logger logger = LoggerFactory.getLogger(RespDecoder.class);

    /**
     * 最大批量字符串长度（512MB）
     */
    private static final int MAX_BULK_LENGTH = 512 * 1024 * 1024;

    /**
     * 解码方法
     * Netty 会在有数据可读时调用此方法
     *
     * @param ctx 通道上下文
     * @param in 输入缓冲区
     * @param out 解码结果列表
     * @throws Exception 解码异常
     */
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // 循环解码，直到没有完整的消息可解码
        while (in.isReadable()) {
            // 标记当前读取位置，如果数据不完整需要回退
            in.markReaderIndex();

            try {
                // 尝试解码一个完整的 RESP 消息
                RespMessage message = decodeMessage(in);

                if (message == null) {
                    // 数据不完整，回退读取位置，等待更多数据
                    in.resetReaderIndex();
                    break;
                }

                // 成功解码，添加到输出列表
                out.add(message);
                logger.debug("解码消息: {}", message);

            } catch (Exception e) {
                // 解码失败，重置读取位置并抛出异常
                in.resetReaderIndex();
                logger.error("解码失败", e);
                throw e;
            }
        }
    }

    /**
     * 解码单个 RESP 消息
     *
     * @param in 输入缓冲区
     * @return 解码的消息，如果数据不完整返回 null
     * @throws Exception 解码异常
     */
    private RespMessage decodeMessage(ByteBuf in) throws Exception {
        // 检查是否有数据可读
        if (!in.isReadable()) {
            return null;
        }

        // 读取类型前缀
        byte prefix = in.readByte();
        RespType type = RespType.fromPrefix(prefix);

        if (type == null) {
            throw new IllegalArgumentException("未知的 RESP 类型前缀: " + (char) prefix);
        }

        // 根据类型解码
        switch (type) {
            case SIMPLE_STRING:
                return decodeSimpleString(in);
            case ERROR:
                return decodeError(in);
            case INTEGER:
                return decodeInteger(in);
            case BULK_STRING:
                return decodeBulkString(in);
            case ARRAY:
                return decodeArray(in);
            default:
                throw new IllegalArgumentException("不支持的 RESP 类型: " + type);
        }
    }

    /**
     * 解码简单字符串
     * 格式：+OK\r\n
     */
    private RespMessage.SimpleString decodeSimpleString(ByteBuf in) {
        String line = readLine(in);
        if (line == null) {
            return null;
        }
        return new RespMessage.SimpleString(line);
    }

    /**
     * 解码错误消息
     * 格式：-Error message\r\n
     */
    private RespMessage.Error decodeError(ByteBuf in) {
        String line = readLine(in);
        if (line == null) {
            return null;
        }
        return new RespMessage.Error(line);
    }

    /**
     * 解码整数
     * 格式：:1000\r\n
     */
    private RespMessage.Integer decodeInteger(ByteBuf in) {
        String line = readLine(in);
        if (line == null) {
            return null;
        }

        try {
            long value = Long.parseLong(line);
            return new RespMessage.Integer(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("无效的整数格式: " + line);
        }
    }

    /**
     * 解码批量字符串
     * 格式：$6\r\nfoobar\r\n 或 $-1\r\n (NULL)
     */
    private RespMessage.BulkString decodeBulkString(ByteBuf in) {
        // 读取长度行
        String lengthStr = readLine(in);
        if (lengthStr == null) {
            return null;
        }

        int length;
        try {
            length = Integer.parseInt(lengthStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("无效的批量字符串长度: " + lengthStr);
        }

        // 处理 NULL bulk string
        if (length == -1) {
            return new RespMessage.BulkString((byte[]) null);
        }

        // 检查长度合法性
        if (length < 0) {
            throw new IllegalArgumentException("批量字符串长度不能为负: " + length);
        }
        if (length > MAX_BULK_LENGTH) {
            throw new IllegalArgumentException("批量字符串长度超过最大限制: " + length);
        }

        // 检查数据是否足够（长度 + \r\n）
        if (in.readableBytes() < length + 2) {
            return null;
        }

        // 读取数据
        byte[] data = new byte[length];
        in.readBytes(data);

        // 读取并验证 CRLF
        byte cr = in.readByte();
        byte lf = in.readByte();
        if (cr != '\r' || lf != '\n') {
            throw new IllegalArgumentException("批量字符串后缺少 CRLF");
        }

        return new RespMessage.BulkString(data);
    }

    /**
     * 解码数组
     * 格式：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
     */
    private RespMessage.Array decodeArray(ByteBuf in) {
        // 读取元素数量
        String countStr = readLine(in);
        if (countStr == null) {
            return null;
        }

        int count;
        try {
            count = Integer.parseInt(countStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("无效的数组元素数量: " + countStr);
        }

        // 处理 NULL array
        if (count == -1) {
            return new RespMessage.Array(null);
        }

        // 检查数量合法性
        if (count < 0) {
            throw new IllegalArgumentException("数组元素数量不能为负: " + count);
        }

        // 解码每个元素
        List<RespMessage> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            // 递归解码元素
            RespMessage element;
            try {
                element = decodeMessage(in);
            } catch (Exception e) {
                throw new IllegalArgumentException("解码数组元素失败: " + e.getMessage(), e);
            }
            if (element == null) {
                // 数据不完整
                return null;
            }
            elements.add(element);
        }

        return new RespMessage.Array(elements);
    }

    /**
     * 读取一行数据（直到 \r\n）
     *
     * @param in 输入缓冲区
     * @return 读取的行内容，如果数据不完整返回 null
     */
    private String readLine(ByteBuf in) {
        // 查找 \r\n 的位置
        int endIndex = findLineEnd(in);
        if (endIndex == -1) {
            // 没有找到完整的行
            return null;
        }

        // 计算行长度
        int lineLength = endIndex - in.readerIndex();

        // 读取行内容
        byte[] lineBytes = new byte[lineLength];
        in.readBytes(lineBytes);

        // 跳过 \r\n
        in.skipBytes(2);

        return new String(lineBytes, StandardCharsets.UTF_8);
    }

    /**
     * 查找行结束位置（\r\n）
     *
     * @param in 输入缓冲区
     * @return \r 的位置，如果未找到返回 -1
     */
    private int findLineEnd(ByteBuf in) {
        int index = in.readerIndex();
        int readableBytes = in.readableBytes();

        // 至少需要 2 个字节（\r\n）
        if (readableBytes < 2) {
            return -1;
        }

        // 查找 \r\n
        for (int i = 0; i < readableBytes - 1; i++) {
            if (in.getByte(index + i) == '\r' && in.getByte(index + i + 1) == '\n') {
                return index + i;
            }
        }

        return -1;
    }

    /**
     * 异常处理
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("解码器异常: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}