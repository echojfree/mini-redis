package com.mini.redis.command;

import com.mini.redis.command.impl.*;
import com.mini.redis.command.list.*;
import com.mini.redis.command.hash.*;
import com.mini.redis.command.set.*;
import com.mini.redis.command.zset.*;
import com.mini.redis.command.transaction.*;
import com.mini.redis.command.pubsub.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Command Factory
 * Manages and creates all Redis commands
 *
 * Design Patterns:
 * 1. Factory Pattern - Creates command objects
 * 2. Singleton Pattern - Global unique command registry
 *
 * @author Mini Redis
 */
public class CommandFactory {

    private static final Logger logger = LoggerFactory.getLogger(CommandFactory.class);

    /**
     * Singleton instance
     */
    private static volatile CommandFactory instance;

    /**
     * Command registry
     * Uses ConcurrentHashMap for thread safety
     */
    private final Map<String, Command> commands;

    /**
     * Private constructor
     */
    private CommandFactory() {
        this.commands = new ConcurrentHashMap<>();
        registerDefaultCommands();
    }

    /**
     * Get factory singleton
     *
     * @return Command factory instance
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
     * Register default commands
     */
    private void registerDefaultCommands() {
        // Basic commands
        register(new PingCommand());

        // String commands
        register(new GetCommand());
        register(new SetCommand());
        register(new IncrCommand());

        // Key commands
        register(new DelCommand());
        register(new ExistsCommand());
        register(new KeysCommand());
        register(new TypeCommand());
        register(new ExpireCommand());
        register(new TtlCommand());

        // Database commands
        register(new SelectCommand());

        // List commands
        register(new LPushCommand());
        register(new RPushCommand());
        register(new LPopCommand());
        register(new RPopCommand());
        register(new LRangeCommand());

        // Hash commands
        register(new HSetCommand());
        register(new HGetCommand());
        register(new HDelCommand());
        register(new HGetAllCommand());

        // Set commands
        register(new SAddCommand());
        register(new SRemCommand());
        register(new SMembersCommand());
        register(new SIsMemberCommand());

        // Sorted Set commands
        register(new ZAddCommand());
        register(new ZRangeCommand());
        register(new ZRemCommand());
        register(new ZScoreCommand());

        // Transaction commands
        register(new MultiCommand());
        register(new ExecCommand());
        register(new DiscardCommand());
        register(new WatchCommand());
        register(new UnwatchCommand());

        // PubSub commands
        register(new PublishCommand());
        register(new SubscribeCommand());
        register(new UnsubscribeCommand());
        register(new PSubscribeCommand());
        register(new PUnsubscribeCommand());

        logger.info("Registered {} commands", commands.size());
    }

    /**
     * Register a command
     *
     * @param command Command to register
     */
    public void register(Command command) {
        if (command == null) {
            throw new IllegalArgumentException("Command cannot be null");
        }

        String name = command.getName();
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Command name cannot be empty");
        }

        Command existing = commands.put(name.toUpperCase(), command);
        if (existing != null) {
            logger.warn("Command {} was overwritten", name);
        } else {
            logger.debug("Registered command: {}", name);
        }
    }

    /**
     * Get a command
     *
     * @param name Command name (case insensitive)
     * @return Command instance, or null if not found
     */
    public Command getCommand(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        return commands.get(name.toUpperCase());
    }

    /**
     * Check if command exists
     *
     * @param name Command name
     * @return true if command exists
     */
    public boolean hasCommand(String name) {
        return getCommand(name) != null;
    }

    /**
     * Get all registered command names
     *
     * @return Command names map
     */
    public Map<String, Command> getAllCommands() {
        return new ConcurrentHashMap<>(commands);
    }

    /**
     * Clear all commands (for testing)
     */
    void clearCommands() {
        commands.clear();
    }

    /**
     * Reload commands
     */
    public void reload() {
        logger.info("Reloading commands...");
        commands.clear();
        registerDefaultCommands();
    }
}