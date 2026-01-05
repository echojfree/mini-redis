package com.mini.redis;

import com.mini.redis.config.RedisConfig;
import com.mini.redis.network.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mini Redis 服务器主类
 * 程序入口点，负责启动和管理整个 Redis 服务器
 *
 * @author Mini Redis
 */
public class MiniRedisServer {

    private static final Logger logger = LoggerFactory.getLogger(MiniRedisServer.class);

    /**
     * 版本信息
     */
    public static final String VERSION = "1.0.0";

    /**
     * Redis 服务器实例
     */
    private RedisServer redisServer;

    /**
     * 启动服务器
     */
    public void start() {
        try {
            logger.info("========================================");
            logger.info("Mini Redis Server v{} 正在启动...", VERSION);
            logger.info("========================================");

            // 初始化配置
            RedisConfig config = RedisConfig.getInstance();
            config.printAllConfigs();

            // 创建并启动服务器
            redisServer = new RedisServer(config);
            redisServer.start();

            logger.info("Mini Redis Server 启动成功！");
            logger.info("监听地址: {}:{}",
                config.getServerBind(),
                config.getServerPort());

            // 添加关闭钩子，优雅关闭
            addShutdownHook();

        } catch (Exception e) {
            logger.error("服务器启动失败", e);
            System.exit(1);
        }
    }

    /**
     * 停止服务器
     */
    public void stop() {
        logger.info("正在停止 Mini Redis Server...");

        if (redisServer != null) {
            try {
                redisServer.stop();
                logger.info("Mini Redis Server 已停止");
            } catch (Exception e) {
                logger.error("停止服务器时发生错误", e);
            }
        }
    }

    /**
     * 添加 JVM 关闭钩子，用于优雅关闭
     */
    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("接收到关闭信号");
            stop();
        }, "shutdown-hook"));
    }

    /**
     * 打印启动横幅
     */
    private static void printBanner() {
        System.out.println();
        System.out.println("╔══════════════════════════════════════╗");
        System.out.println("║      Mini Redis Server v" + VERSION + "       ║");
        System.out.println("║   A Redis Implementation for Study   ║");
        System.out.println("╚══════════════════════════════════════╝");
        System.out.println();
    }

    /**
     * 主函数入口
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        printBanner();

        // 创建并启动服务器
        MiniRedisServer server = new MiniRedisServer();
        server.start();

        // 保持主线程运行
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            logger.warn("主线程被中断", e);
            Thread.currentThread().interrupt();
        }
    }
}