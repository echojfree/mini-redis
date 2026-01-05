package com.mini.redis;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import static org.junit.Assert.*;

/**
 * Mini Redis 服务器集成测试
 * 测试服务器基本功能
 *
 * @author Mini Redis
 */
public class MiniRedisServerTest {

    /**
     * 测试 PING 命令
     * 验证服务器能够正确响应 PING 命令
     */
    @Test
    public void testPingCommand() throws Exception {
        // 启动服务器（在实际测试中，应该在 @Before 中启动）
        Thread serverThread = new Thread(() -> {
            MiniRedisServer.main(new String[]{});
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // 等待服务器启动
        Thread.sleep(2000);

        // 连接到服务器
        try (Socket socket = new Socket("localhost", 6379);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 发送 PING 命令（RESP 格式）
            // *1\r\n$4\r\nPING\r\n
            out.print("*1\r\n$4\r\nPING\r\n");
            out.flush();

            // 读取响应
            String response = in.readLine();

            // 验证响应是否为 +PONG
            assertNotNull("应该收到响应", response);
            assertEquals("应该返回 +PONG", "+PONG", response);

            System.out.println("PING 命令测试通过！");
        }
    }

    /**
     * 测试带参数的 PING 命令
     */
    @Test
    public void testPingWithMessage() throws Exception {
        // 启动服务器
        Thread serverThread = new Thread(() -> {
            MiniRedisServer.main(new String[]{});
        });
        serverThread.setDaemon(true);
        serverThread.start();

        // 等待服务器启动
        Thread.sleep(2000);

        try (Socket socket = new Socket("localhost", 6379);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // 发送带参数的 PING 命令
            // *2\r\n$4\r\nPING\r\n$5\r\nhello\r\n
            out.print("*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n");
            out.flush();

            // 读取响应（批量字符串格式）
            String lengthLine = in.readLine();
            assertNotNull("应该收到长度行", lengthLine);
            assertEquals("长度应该是 $5", "$5", lengthLine);

            String message = in.readLine();
            assertEquals("应该返回 hello", "hello", message);

            System.out.println("带参数的 PING 命令测试通过！");
        }
    }
}