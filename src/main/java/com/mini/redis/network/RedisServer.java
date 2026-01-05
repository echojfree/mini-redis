package com.mini.redis.network;

import com.mini.redis.config.RedisConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Redis 服务器网络层实现
 * 使用 Netty 实现高性能的异步网络 IO
 *
 * 核心知识点：
 * 1. Netty 事件驱动模型
 * 2. Reactor 模式（主从 Reactor）
 * 3. NIO 非阻塞 IO
 * 4. 线程池模型
 *
 * @author Mini Redis
 */
public class RedisServer {

    private static final Logger logger = LoggerFactory.getLogger(RedisServer.class);

    /**
     * 配置管理器
     */
    private final RedisConfig config;

    /**
     * Boss 线程组（主 Reactor）
     * 负责接受新的连接请求
     */
    private EventLoopGroup bossGroup;

    /**
     * Worker 线程组（从 Reactor）
     * 负责处理已建立连接的 IO 操作
     */
    private EventLoopGroup workerGroup;

    /**
     * 服务器通道
     */
    private Channel serverChannel;

    /**
     * 服务器状态
     */
    private volatile boolean isRunning = false;

    /**
     * 构造函数
     *
     * @param config 配置管理器
     */
    public RedisServer(RedisConfig config) {
        this.config = config;
    }

    /**
     * 启动服务器
     *
     * @throws InterruptedException 如果启动过程被中断
     */
    public void start() throws InterruptedException {
        if (isRunning) {
            logger.warn("服务器已经在运行中");
            return;
        }

        // 创建 Boss 线程组（通常 1 个线程就够了）
        bossGroup = new NioEventLoopGroup(1);

        // 创建 Worker 线程组（根据配置决定线程数）
        int workerThreads = config.getWorkerThreads();
        workerGroup = new NioEventLoopGroup(workerThreads);

        try {
            // 创建服务器启动器
            ServerBootstrap bootstrap = new ServerBootstrap();

            // 配置服务器
            bootstrap.group(bossGroup, workerGroup)
                    // 指定使用 NIO 传输方式
                    .channel(NioServerSocketChannel.class)
                    // 设置服务器套接字选项
                    .option(ChannelOption.SO_BACKLOG, config.getInt("network.so-backlog", 511))
                    .option(ChannelOption.SO_REUSEADDR, true)
                    // 设置子通道选项（客户端连接）
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, config.isTcpNoDelay())
                    .childOption(ChannelOption.SO_RCVBUF, 32 * 1024)
                    .childOption(ChannelOption.SO_SNDBUF, 32 * 1024)
                    // 设置通道初始化器
                    .childHandler(new RedisChannelInitializer(config));

            // 绑定端口并启动服务器
            String bindAddress = config.getServerBind();
            int port = config.getServerPort();
            InetSocketAddress socketAddress = new InetSocketAddress(bindAddress, port);

            // 同步等待绑定完成
            ChannelFuture bindFuture = bootstrap.bind(socketAddress).sync();

            if (bindFuture.isSuccess()) {
                serverChannel = bindFuture.channel();
                isRunning = true;
                logger.info("服务器成功绑定到 {}:{}", bindAddress, port);

                // 添加监听器，在通道关闭时进行清理
                serverChannel.closeFuture().addListener((ChannelFutureListener) future -> {
                    logger.info("服务器通道已关闭");
                    isRunning = false;
                });
            } else {
                throw new RuntimeException("无法绑定到指定地址和端口");
            }

        } catch (Exception e) {
            logger.error("启动服务器失败", e);
            // 清理资源
            stop();
            throw e;
        }
    }

    /**
     * 停止服务器
     */
    public void stop() {
        if (!isRunning && serverChannel == null) {
            return;
        }

        isRunning = false;

        try {
            // 关闭服务器通道
            if (serverChannel != null) {
                serverChannel.close().sync();
                serverChannel = null;
            }
        } catch (InterruptedException e) {
            logger.warn("关闭服务器通道时被中断", e);
            Thread.currentThread().interrupt();
        } finally {
            // 优雅关闭线程组
            if (bossGroup != null) {
                bossGroup.shutdownGracefully();
                bossGroup = null;
            }

            if (workerGroup != null) {
                workerGroup.shutdownGracefully();
                workerGroup = null;
            }

            logger.info("服务器已停止");
        }
    }

    /**
     * 检查服务器是否正在运行
     *
     * @return 如果服务器正在运行返回 true
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * 获取当前连接数
     *
     * @return 当前活跃的客户端连接数
     */
    public int getConnectionCount() {
        // TODO: 实现连接计数
        return 0;
    }
}