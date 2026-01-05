package com.mini.redis.storage;

import com.mini.redis.server.RedisClient;
import com.mini.redis.command.impl.*;
import com.mini.redis.protocol.RespMessage;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * 存储引擎测试
 * 测试 Redis 的基础存储功能和命令
 *
 * @author Mini Redis
 */
public class StorageEngineTest {

    @Mock
    private Channel mockChannel;

    private RedisClient client;
    private SetCommand setCommand;
    private GetCommand getCommand;
    private DelCommand delCommand;
    private ExistsCommand existsCommand;
    private IncrCommand incrCommand;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        // 创建模拟客户端
        when(mockChannel.remoteAddress()).thenReturn(null);
        client = new RedisClient(mockChannel);

        // 初始化命令
        setCommand = new SetCommand();
        getCommand = new GetCommand();
        delCommand = new DelCommand();
        existsCommand = new ExistsCommand();
        incrCommand = new IncrCommand();
    }

    /**
     * 测试基本的 SET 和 GET
     */
    @Test
    public void testSetAndGet() {
        // SET mykey "hello"
        RespMessage setResult = setCommand.execute(
            Arrays.asList("mykey", "hello"), client);

        assertTrue(setResult instanceof RespMessage.SimpleString);
        assertEquals("OK", ((RespMessage.SimpleString) setResult).getValue());

        // GET mykey
        RespMessage getResult = getCommand.execute(
            Collections.singletonList("mykey"), client);

        assertTrue(getResult instanceof RespMessage.BulkString);
        assertEquals("hello", ((RespMessage.BulkString) getResult).getStringValue());
    }

    /**
     * 测试 GET 不存在的键
     */
    @Test
    public void testGetNonExistent() {
        RespMessage result = getCommand.execute(
            Collections.singletonList("nonexistent"), client);

        assertTrue(result instanceof RespMessage.BulkString);
        assertTrue(((RespMessage.BulkString) result).isNull());
    }

    /**
     * 测试 DEL 命令
     */
    @Test
    public void testDel() {
        // 先设置一些键
        setCommand.execute(Arrays.asList("key1", "value1"), client);
        setCommand.execute(Arrays.asList("key2", "value2"), client);
        setCommand.execute(Arrays.asList("key3", "value3"), client);

        // 删除键
        RespMessage result = delCommand.execute(
            Arrays.asList("key1", "key2", "nonexistent"), client);

        assertTrue(result instanceof RespMessage.Integer);
        assertEquals(2, ((RespMessage.Integer) result).getValue());

        // 验证键已删除
        RespMessage get1 = getCommand.execute(Collections.singletonList("key1"), client);
        assertTrue(((RespMessage.BulkString) get1).isNull());

        RespMessage get3 = getCommand.execute(Collections.singletonList("key3"), client);
        assertEquals("value3", ((RespMessage.BulkString) get3).getStringValue());
    }

    /**
     * 测试 EXISTS 命令
     */
    @Test
    public void testExists() {
        // 设置一个键
        setCommand.execute(Arrays.asList("mykey", "myvalue"), client);

        // 测试存在的键
        RespMessage result1 = existsCommand.execute(
            Collections.singletonList("mykey"), client);
        assertEquals(1, ((RespMessage.Integer) result1).getValue());

        // 测试不存在的键
        RespMessage result2 = existsCommand.execute(
            Collections.singletonList("nonexistent"), client);
        assertEquals(0, ((RespMessage.Integer) result2).getValue());

        // 测试多个键
        RespMessage result3 = existsCommand.execute(
            Arrays.asList("mykey", "nonexistent", "mykey"), client);
        assertEquals(2, ((RespMessage.Integer) result3).getValue());
    }

    /**
     * 测试 INCR 命令
     */
    @Test
    public void testIncr() {
        // 对不存在的键执行 INCR
        RespMessage result1 = incrCommand.execute(
            Collections.singletonList("counter"), client);
        assertEquals(1, ((RespMessage.Integer) result1).getValue());

        // 再次 INCR
        RespMessage result2 = incrCommand.execute(
            Collections.singletonList("counter"), client);
        assertEquals(2, ((RespMessage.Integer) result2).getValue());

        // 验证值
        RespMessage getResult = getCommand.execute(
            Collections.singletonList("counter"), client);
        assertEquals("2", ((RespMessage.BulkString) getResult).getStringValue());
    }

    /**
     * 测试带过期时间的 SET
     */
    @Test
    public void testSetWithExpire() throws InterruptedException {
        // SET mykey "hello" EX 1
        RespMessage setResult = setCommand.execute(
            Arrays.asList("mykey", "hello", "EX", "1"), client);
        assertEquals("OK", ((RespMessage.SimpleString) setResult).getValue());

        // 立即获取，应该存在
        RespMessage getResult1 = getCommand.execute(
            Collections.singletonList("mykey"), client);
        assertEquals("hello", ((RespMessage.BulkString) getResult1).getStringValue());

        // 等待 2 秒
        Thread.sleep(2000);

        // 再次获取，应该已过期
        RespMessage getResult2 = getCommand.execute(
            Collections.singletonList("mykey"), client);
        assertTrue(((RespMessage.BulkString) getResult2).isNull());
    }

    /**
     * 测试 SET NX（仅当键不存在时设置）
     */
    @Test
    public void testSetNX() {
        // 第一次 SET NX，应该成功
        RespMessage result1 = setCommand.execute(
            Arrays.asList("mykey", "value1", "NX"), client);
        assertEquals("OK", ((RespMessage.SimpleString) result1).getValue());

        // 第二次 SET NX，应该失败
        RespMessage result2 = setCommand.execute(
            Arrays.asList("mykey", "value2", "NX"), client);
        assertTrue(result2 instanceof RespMessage.BulkString);
        assertTrue(((RespMessage.BulkString) result2).isNull());

        // 验证值没有改变
        RespMessage getResult = getCommand.execute(
            Collections.singletonList("mykey"), client);
        assertEquals("value1", ((RespMessage.BulkString) getResult).getStringValue());
    }

    /**
     * 测试 SET XX（仅当键存在时设置）
     */
    @Test
    public void testSetXX() {
        // 对不存在的键执行 SET XX，应该失败
        RespMessage result1 = setCommand.execute(
            Arrays.asList("mykey", "value1", "XX"), client);
        assertTrue(result1 instanceof RespMessage.BulkString);
        assertTrue(((RespMessage.BulkString) result1).isNull());

        // 先设置键
        setCommand.execute(Arrays.asList("mykey", "value1"), client);

        // 再执行 SET XX，应该成功
        RespMessage result2 = setCommand.execute(
            Arrays.asList("mykey", "value2", "XX"), client);
        assertEquals("OK", ((RespMessage.SimpleString) result2).getValue());

        // 验证值已更新
        RespMessage getResult = getCommand.execute(
            Collections.singletonList("mykey"), client);
        assertEquals("value2", ((RespMessage.BulkString) getResult).getStringValue());
    }
}