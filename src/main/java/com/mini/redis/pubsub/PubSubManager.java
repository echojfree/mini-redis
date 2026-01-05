package com.mini.redis.pubsub;

import com.mini.redis.protocol.RespMessage;
import com.mini.redis.server.RedisClient;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis 发布订阅管理器
 *
 * 实现 Redis 的发布订阅功能：
 * - SUBSCRIBE：订阅频道
 * - UNSUBSCRIBE：取消订阅
 * - PUBLISH：发布消息
 * - PSUBSCRIBE：订阅模式
 * - PUNSUBSCRIBE：取消模式订阅
 *
 * 面试知识点：
 * 1. 观察者模式（Observer Pattern）
 * 2. 发布订阅模式 vs 消息队列
 * 3. 线程安全的订阅者管理
 * 4. 模式匹配算法
 * 5. 消息广播机制
 *
 * @author Mini Redis
 */
public class PubSubManager {

    private static final Logger logger = LoggerFactory.getLogger(PubSubManager.class);

    /**
     * 订阅者信息
     */
    public static class Subscription {
        private final RedisClient client;
        private final String pattern; // null for exact channel, pattern for pattern subscription
        private final long timestamp;

        public Subscription(RedisClient client, String pattern) {
            this.client = client;
            this.pattern = pattern;
            this.timestamp = System.currentTimeMillis();
        }

        public RedisClient getClient() {
            return client;
        }

        public String getPattern() {
            return pattern;
        }

        public boolean isPatternSubscription() {
            return pattern != null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Subscription that = (Subscription) o;
            return Objects.equals(client, that.client) &&
                   Objects.equals(pattern, that.pattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(client, pattern);
        }
    }

    // 单例实例
    private static volatile PubSubManager instance;

    // 频道订阅者映射：channel -> subscribers
    private final ConcurrentHashMap<String, CopyOnWriteArraySet<Subscription>> channelSubscribers;

    // 模式订阅者映射：pattern -> subscribers
    private final ConcurrentHashMap<String, CopyOnWriteArraySet<Subscription>> patternSubscribers;

    // 客户端订阅映射：client -> subscribed channels
    private final ConcurrentHashMap<RedisClient, Set<String>> clientChannels;

    // 客户端模式订阅映射：client -> subscribed patterns
    private final ConcurrentHashMap<RedisClient, Set<String>> clientPatterns;

    // 统计信息
    private final AtomicLong totalMessages = new AtomicLong(0);
    private final AtomicLong totalSubscriptions = new AtomicLong(0);

    /**
     * 私有构造函数
     */
    private PubSubManager() {
        this.channelSubscribers = new ConcurrentHashMap<>();
        this.patternSubscribers = new ConcurrentHashMap<>();
        this.clientChannels = new ConcurrentHashMap<>();
        this.clientPatterns = new ConcurrentHashMap<>();
    }

    /**
     * 获取单例实例
     */
    public static PubSubManager getInstance() {
        if (instance == null) {
            synchronized (PubSubManager.class) {
                if (instance == null) {
                    instance = new PubSubManager();
                }
            }
        }
        return instance;
    }

    /**
     * 订阅频道
     */
    public int subscribe(RedisClient client, String... channels) {
        int count = 0;
        Set<String> clientSubs = clientChannels.computeIfAbsent(client, k -> new HashSet<>());

        for (String channel : channels) {
            CopyOnWriteArraySet<Subscription> subs = channelSubscribers.computeIfAbsent(
                channel, k -> new CopyOnWriteArraySet<>()
            );

            Subscription subscription = new Subscription(client, null);
            if (subs.add(subscription)) {
                clientSubs.add(channel);
                count++;
                totalSubscriptions.incrementAndGet();
                logger.info("Client {} subscribed to channel: {}", client.getId(), channel);

                // Send subscription confirmation
                sendSubscriptionMessage(client, "subscribe", channel, clientSubs.size());
            }
        }

        return count;
    }

    /**
     * 取消订阅频道
     */
    public int unsubscribe(RedisClient client, String... channels) {
        Set<String> clientSubs = clientChannels.get(client);
        if (clientSubs == null) {
            return 0;
        }

        int count = 0;
        List<String> channelsToUnsubscribe = channels.length == 0
            ? new ArrayList<>(clientSubs)
            : Arrays.asList(channels);

        for (String channel : channelsToUnsubscribe) {
            CopyOnWriteArraySet<Subscription> subs = channelSubscribers.get(channel);
            if (subs != null) {
                Subscription subscription = new Subscription(client, null);
                if (subs.remove(subscription)) {
                    clientSubs.remove(channel);
                    count++;
                    totalSubscriptions.decrementAndGet();
                    logger.info("Client {} unsubscribed from channel: {}", client.getId(), channel);

                    // Send unsubscription confirmation
                    sendSubscriptionMessage(client, "unsubscribe", channel, clientSubs.size());

                    // Clean up empty sets
                    if (subs.isEmpty()) {
                        channelSubscribers.remove(channel);
                    }
                }
            }
        }

        if (clientSubs.isEmpty()) {
            clientChannels.remove(client);
        }

        return count;
    }

    /**
     * 订阅模式
     */
    public int psubscribe(RedisClient client, String... patterns) {
        int count = 0;
        Set<String> clientPats = clientPatterns.computeIfAbsent(client, k -> new HashSet<>());

        for (String pattern : patterns) {
            CopyOnWriteArraySet<Subscription> subs = patternSubscribers.computeIfAbsent(
                pattern, k -> new CopyOnWriteArraySet<>()
            );

            Subscription subscription = new Subscription(client, pattern);
            if (subs.add(subscription)) {
                clientPats.add(pattern);
                count++;
                totalSubscriptions.incrementAndGet();
                logger.info("Client {} subscribed to pattern: {}", client.getId(), pattern);

                // Send pattern subscription confirmation
                Set<String> clientSubs = clientChannels.getOrDefault(client, Collections.emptySet());
                sendSubscriptionMessage(client, "psubscribe", pattern,
                    clientSubs.size() + clientPats.size());
            }
        }

        return count;
    }

    /**
     * 取消模式订阅
     */
    public int punsubscribe(RedisClient client, String... patterns) {
        Set<String> clientPats = clientPatterns.get(client);
        if (clientPats == null) {
            return 0;
        }

        int count = 0;
        List<String> patternsToUnsubscribe = patterns.length == 0
            ? new ArrayList<>(clientPats)
            : Arrays.asList(patterns);

        for (String pattern : patternsToUnsubscribe) {
            CopyOnWriteArraySet<Subscription> subs = patternSubscribers.get(pattern);
            if (subs != null) {
                Subscription subscription = new Subscription(client, pattern);
                if (subs.remove(subscription)) {
                    clientPats.remove(pattern);
                    count++;
                    totalSubscriptions.decrementAndGet();
                    logger.info("Client {} unsubscribed from pattern: {}", client.getId(), pattern);

                    Set<String> clientSubs = clientChannels.getOrDefault(client, Collections.emptySet());
                    sendSubscriptionMessage(client, "punsubscribe", pattern,
                        clientSubs.size() + clientPats.size());

                    // Clean up empty sets
                    if (subs.isEmpty()) {
                        patternSubscribers.remove(pattern);
                    }
                }
            }
        }

        if (clientPats.isEmpty()) {
            clientPatterns.remove(client);
        }

        return count;
    }

    /**
     * 发布消息到频道
     */
    public int publish(String channel, String message) {
        int receivers = 0;
        totalMessages.incrementAndGet();

        // Send to exact channel subscribers
        CopyOnWriteArraySet<Subscription> channelSubs = channelSubscribers.get(channel);
        if (channelSubs != null) {
            for (Subscription sub : channelSubs) {
                sendMessage(sub.getClient(), "message", channel, message, null);
                receivers++;
            }
        }

        // Send to pattern subscribers
        for (Map.Entry<String, CopyOnWriteArraySet<Subscription>> entry : patternSubscribers.entrySet()) {
            String pattern = entry.getKey();
            if (matchPattern(pattern, channel)) {
                for (Subscription sub : entry.getValue()) {
                    sendMessage(sub.getClient(), "pmessage", channel, message, pattern);
                    receivers++;
                }
            }
        }

        logger.info("Published message to channel '{}', {} receivers", channel, receivers);
        return receivers;
    }

    /**
     * 发送订阅/取消订阅确认消息
     */
    private void sendSubscriptionMessage(RedisClient client, String type, String channel, int count) {
        try {
            List<RespMessage> elements = new ArrayList<>();
            elements.add(new RespMessage.BulkString(type));
            elements.add(new RespMessage.BulkString(channel));
            elements.add(new RespMessage.Integer(count));

            RespMessage response = new RespMessage.Array(elements);
            Channel ch = client.getChannel();
            if (ch != null && ch.isActive()) {
                ch.writeAndFlush(response);
            }
        } catch (Exception e) {
            logger.error("Failed to send subscription message to client {}", client.getId(), e);
        }
    }

    /**
     * 发送消息给订阅者
     */
    private void sendMessage(RedisClient client, String type, String channel, String message, String pattern) {
        try {
            List<RespMessage> elements = new ArrayList<>();
            elements.add(new RespMessage.BulkString(type));

            if ("pmessage".equals(type)) {
                // Pattern message format: ["pmessage", pattern, channel, message]
                elements.add(new RespMessage.BulkString(pattern));
                elements.add(new RespMessage.BulkString(channel));
            } else {
                // Regular message format: ["message", channel, message]
                elements.add(new RespMessage.BulkString(channel));
            }

            elements.add(new RespMessage.BulkString(message));

            RespMessage response = new RespMessage.Array(elements);
            Channel ch = client.getChannel();
            if (ch != null && ch.isActive()) {
                ch.writeAndFlush(response);
            }
        } catch (Exception e) {
            logger.error("Failed to send message to client {}", client.getId(), e);
        }
    }

    /**
     * 模式匹配
     * 支持 * 和 ? 通配符
     */
    private boolean matchPattern(String pattern, String channel) {
        // Simple glob pattern matching
        String regex = pattern
            .replace(".", "\\.")
            .replace("*", ".*")
            .replace("?", ".");
        return channel.matches(regex);
    }

    /**
     * 客户端断开时清理订阅
     */
    public void removeClient(RedisClient client) {
        // Remove from channel subscriptions
        Set<String> channels = clientChannels.remove(client);
        if (channels != null) {
            for (String channel : channels) {
                CopyOnWriteArraySet<Subscription> subs = channelSubscribers.get(channel);
                if (subs != null) {
                    subs.removeIf(sub -> sub.getClient().equals(client));
                    if (subs.isEmpty()) {
                        channelSubscribers.remove(channel);
                    }
                }
            }
        }

        // Remove from pattern subscriptions
        Set<String> patterns = clientPatterns.remove(client);
        if (patterns != null) {
            for (String pattern : patterns) {
                CopyOnWriteArraySet<Subscription> subs = patternSubscribers.get(pattern);
                if (subs != null) {
                    subs.removeIf(sub -> sub.getClient().equals(client));
                    if (subs.isEmpty()) {
                        patternSubscribers.remove(pattern);
                    }
                }
            }
        }

        logger.info("Cleaned up subscriptions for client {}", client.getId());
    }

    /**
     * 检查客户端是否处于订阅模式
     */
    public boolean isInPubSubMode(RedisClient client) {
        return clientChannels.containsKey(client) || clientPatterns.containsKey(client);
    }

    /**
     * 获取客户端订阅的频道数
     */
    public int getSubscriptionCount(RedisClient client) {
        int count = 0;
        Set<String> channels = clientChannels.get(client);
        if (channels != null) {
            count += channels.size();
        }
        Set<String> patterns = clientPatterns.get(client);
        if (patterns != null) {
            count += patterns.size();
        }
        return count;
    }

    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("total_channels", channelSubscribers.size());
        stats.put("total_patterns", patternSubscribers.size());
        stats.put("total_subscriptions", totalSubscriptions.get());
        stats.put("total_messages", totalMessages.get());
        stats.put("active_clients", clientChannels.size() + clientPatterns.size());
        return stats;
    }

    /**
     * 清理所有订阅（用于测试）
     */
    void clearAll() {
        channelSubscribers.clear();
        patternSubscribers.clear();
        clientChannels.clear();
        clientPatterns.clear();
        totalMessages.set(0);
        totalSubscriptions.set(0);
    }
}