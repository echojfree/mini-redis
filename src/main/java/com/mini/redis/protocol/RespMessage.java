package com.mini.redis.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * RESP 协议消息基类
 * 所有 RESP 消息类型的抽象基类
 *
 * @author Mini Redis
 */
public abstract class RespMessage {

    /**
     * 消息类型
     */
    protected final RespType type;

    /**
     * CRLF 分隔符
     */
    protected static final byte[] CRLF = new byte[]{'\r', '\n'};

    /**
     * 构造函数
     *
     * @param type 消息类型
     */
    protected RespMessage(RespType type) {
        this.type = type;
    }

    /**
     * 获取消息类型
     *
     * @return 消息类型
     */
    public RespType getType() {
        return type;
    }

    /**
     * 将消息编码到 ByteBuf
     *
     * @param out 输出缓冲区
     */
    public abstract void encode(ByteBuf out);

    /**
     * 简单字符串消息
     * 格式：+OK\r\n
     */
    public static class SimpleString extends RespMessage {
        private final String value;

        public SimpleString(String value) {
            super(RespType.SIMPLE_STRING);
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte(type.getPrefix());
            out.writeBytes(value.getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        }

        @Override
        public String toString() {
            return "SimpleString{" + value + "}";
        }
    }

    /**
     * 错误消息
     * 格式：-ERR unknown command\r\n
     */
    public static class Error extends RespMessage {
        private final String message;

        public Error(String message) {
            super(RespType.ERROR);
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte(type.getPrefix());
            out.writeBytes(message.getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        }

        @Override
        public String toString() {
            return "Error{" + message + "}";
        }
    }

    /**
     * 整数消息
     * 格式：:1000\r\n
     */
    public static class Integer extends RespMessage {
        private final long value;

        public Integer(long value) {
            super(RespType.INTEGER);
            this.value = value;
        }

        public long getValue() {
            return value;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte(type.getPrefix());
            out.writeBytes(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
            out.writeBytes(CRLF);
        }

        @Override
        public String toString() {
            return "Integer{" + value + "}";
        }
    }

    /**
     * 批量字符串消息
     * 格式：$6\r\nfoobar\r\n 或 $-1\r\n (NULL)
     */
    public static class BulkString extends RespMessage {
        private final byte[] value;

        public BulkString(byte[] value) {
            super(RespType.BULK_STRING);
            this.value = value;
        }

        public BulkString(String value) {
            this(value != null ? value.getBytes(StandardCharsets.UTF_8) : null);
        }

        public byte[] getValue() {
            return value;
        }

        public String getStringValue() {
            return value != null ? new String(value, StandardCharsets.UTF_8) : null;
        }

        public boolean isNull() {
            return value == null;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte(type.getPrefix());
            if (value == null) {
                // NULL bulk string
                out.writeBytes("-1".getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
            } else {
                // 写入长度
                out.writeBytes(String.valueOf(value.length).getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
                // 写入数据
                out.writeBytes(value);
                out.writeBytes(CRLF);
            }
        }

        @Override
        public String toString() {
            return "BulkString{" + getStringValue() + "}";
        }
    }

    /**
     * 数组消息
     * 格式：*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
     */
    public static class Array extends RespMessage {
        private final List<RespMessage> elements;

        public Array(List<RespMessage> elements) {
            super(RespType.ARRAY);
            this.elements = elements != null ? elements : new ArrayList<>();
        }

        public Array() {
            this(new ArrayList<>());
        }

        public List<RespMessage> getElements() {
            return elements;
        }

        public void addElement(RespMessage element) {
            elements.add(element);
        }

        public int size() {
            return elements.size();
        }

        public boolean isEmpty() {
            return elements.isEmpty();
        }

        public boolean isNull() {
            return elements == null;
        }

        @Override
        public void encode(ByteBuf out) {
            out.writeByte(type.getPrefix());
            if (elements == null) {
                // NULL array
                out.writeBytes("-1".getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
            } else {
                // 写入元素数量
                out.writeBytes(String.valueOf(elements.size()).getBytes(StandardCharsets.UTF_8));
                out.writeBytes(CRLF);
                // 递归编码每个元素
                for (RespMessage element : elements) {
                    element.encode(out);
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("Array[");
            if (elements != null) {
                for (int i = 0; i < elements.size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(elements.get(i));
                }
            }
            sb.append("]");
            return sb.toString();
        }
    }

    /**
     * 创建 OK 响应
     */
    public static SimpleString ok() {
        return new SimpleString("OK");
    }

    /**
     * 创建 PONG 响应
     */
    public static SimpleString pong() {
        return new SimpleString("PONG");
    }

    /**
     * 创建错误响应
     */
    public static Error error(String message) {
        return new Error("ERR " + message);
    }

    /**
     * 创建 NULL 批量字符串
     */
    public static BulkString nullBulkString() {
        return new BulkString((byte[]) null);
    }

    /**
     * 创建空数组
     */
    public static Array emptyArray() {
        return new Array(new ArrayList<>());
    }
}