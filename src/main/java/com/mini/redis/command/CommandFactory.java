package com.mini.redis.command;

import com.mini.redis.command.impl.PingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 命令工厂
 * 负责管理和创建所有的 Redis 命令
 *
 * 设计模式：
 * 1. 工厂模式（Factory Pattern）- 创建命令对象
 * 2. 单例模式（Singleton Pattern）- 全局唯一的命令注册表
 *
 * @author Mini Redis
 */
public class CommandFactory {

    private static final Logger logger = LoggerFactory.getLogger(CommandFactory.class);

    /**
     * 单例实例
     */
    private static volatile CommandFactory instance;

    /**
     * 命令注册表
     * 使用 ConcurrentHashMap 保证线程安全
     */
    private final Map<String, Command> commands;

    /**
     * 私有构造函数
     */
    private CommandFactory() {
        this.commands = new ConcurrentHashMap<>();
        registerDefaultCommands();
    }

    /**
     * 获取工厂单例
     *
     * @return 命令工厂实例
     */
    public static CommandFactory getInstance() {
        if (instance == null) {
            synchronized (CommandFactory.class) {
                if (instance == null) {
                    instance = new CommandFactory();
                }
            }
        }
        return instance;
    }

    /**
     * 注册默认命令
     */
    private void registerDefaultCommands() {
        // 注册基础命令
        register(new PingCommand());

        // TODO: 注册其他命令
        // register(new GetCommand());
        // register(new SetCommand());
        // register(new DelCommand());
        // register(new ExistsCommand());
        // register(new KeysCommand());
        // register(new TypeCommand());
        // register(new ExpireCommand());
        // register(new TtlCommand());
        // register(new FlushDbCommand());
        // register(new FlushAllCommand());

        logger.info("注册了 {} 个命令", commands.size());
    }

    /**
     * 注册命令
     *
     * @param command 要注册的命令
     */
    public void register(Command command) {
        if (command == null) {
            throw new IllegalArgumentException("命令不能为空");
        }

        String name = command.getName();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("命令名不能为空");
        }

        Command existing = commands.put(name.toUpperCase(), command);
        if (existing != null) {
            logger.warn("命令 {} 被覆盖注册", name);
        } else {
            logger.debug("注册命令: {}", name);
        }
    }

    /**
     * 获取命令
     *
     * @param name 命令名（不区分大小写）
     * @return 命令实例，如果不存在返回 null
     */
    public Command getCommand(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        return commands.get(name.toUpperCase());
    }

    /**
     * 检查命令是否存在
     *
     * @param name 命令名
     * @return 如果命令存在返回 true
     */
    public boolean hasCommand(String name) {
        return getCommand(name) != null;
    }

    /**
     * 获取所有已注册的命令名
     *
     * @return 命令名集合
     */
    public Map<String, Command> getAllCommands() {
        return new ConcurrentHashMap<>(commands);
    }

    /**
     * 清空所有命令（用于测试）
     */
    void clearCommands() {
        commands.clear();
    }

    /**
     * 重新加载命令
     */
    public void reload() {
        logger.info("重新加载命令...");
        commands.clear();
        registerDefaultCommands();
    }
}