package com.mini.redis.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

/**
 * RESP 协议消息测试
 * 测试各种 RESP 消息类型的编码功能
 *
 * @author Mini Redis
 */
public class RespMessageTest {

    /**
     * 测试简单字符串编码
     */
    @Test
    public void testSimpleStringEncode() {
        RespMessage.SimpleString message = new RespMessage.SimpleString("OK");
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);

        String encoded = buf.toString(StandardCharsets.UTF_8);
        assertEquals("+OK\r\n", encoded);
    }

    /**
     * 测试错误消息编码
     */
    @Test
    public void testErrorEncode() {
        RespMessage.Error message = new RespMessage.Error("ERR unknown command");
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);

        String encoded = buf.toString(StandardCharsets.UTF_8);
        assertEquals("-ERR unknown command\r\n", encoded);
    }

    /**
     * 测试整数编码
     */
    @Test
    public void testIntegerEncode() {
        RespMessage.Integer message = new RespMessage.Integer(1000);
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);

        String encoded = buf.toString(StandardCharsets.UTF_8);
        assertEquals(":1000\r\n", encoded);
    }

    /**
     * 测试批量字符串编码
     */
    @Test
    public void testBulkStringEncode() {
        RespMessage.BulkString message = new RespMessage.BulkString("foobar");
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);

        String encoded = buf.toString(StandardCharsets.UTF_8);
        assertEquals("$6\r\nfoobar\r\n", encoded);
    }

    /**
     * 测试 NULL 批量字符串编码
     */
    @Test
    public void testNullBulkStringEncode() {
        RespMessage.BulkString message = RespMessage.nullBulkString();
        ByteBuf buf = Unpooled.buffer();
        message.encode(buf);

        String encoded = buf.toString(StandardCharsets.UTF_8);
        assertEquals("$-1\r\n", encoded);
    }

    /**
     * 测试数组编码
     */
    @Test
    public void testArrayEncode() {
        RespMessage.Array array = new RespMessage.Array();
        array.addElement(new RespMessage.BulkString("foo"));
        array.addElement(new RespMessage.BulkString("bar"));

        ByteBuf buf = Unpooled.buffer();
        array.encode(buf);

        String encoded = buf.toString(StandardCharsets.UTF_8);
        assertEquals("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n", encoded);
    }

    /**
     * 测试空数组编码
     */
    @Test
    public void testEmptyArrayEncode() {
        RespMessage.Array array = RespMessage.emptyArray();
        ByteBuf buf = Unpooled.buffer();
        array.encode(buf);

        String encoded = buf.toString(StandardCharsets.UTF_8);
        assertEquals("*0\r\n", encoded);
    }

    /**
     * 测试嵌套数组编码
     */
    @Test
    public void testNestedArrayEncode() {
        RespMessage.Array innerArray = new RespMessage.Array();
        innerArray.addElement(new RespMessage.Integer(1));
        innerArray.addElement(new RespMessage.Integer(2));

        RespMessage.Array outerArray = new RespMessage.Array();
        outerArray.addElement(new RespMessage.BulkString("foo"));
        outerArray.addElement(innerArray);

        ByteBuf buf = Unpooled.buffer();
        outerArray.encode(buf);

        String encoded = buf.toString(StandardCharsets.UTF_8);
        assertEquals("*2\r\n$3\r\nfoo\r\n*2\r\n:1\r\n:2\r\n", encoded);
    }

    /**
     * 测试工具方法
     */
    @Test
    public void testUtilityMethods() {
        // 测试 ok() 方法
        RespMessage.SimpleString ok = RespMessage.ok();
        assertEquals("OK", ok.getValue());

        // 测试 pong() 方法
        RespMessage.SimpleString pong = RespMessage.pong();
        assertEquals("PONG", pong.getValue());

        // 测试 error() 方法
        RespMessage.Error error = RespMessage.error("test error");
        assertEquals("ERR test error", error.getMessage());
    }
}