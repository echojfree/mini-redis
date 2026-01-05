package com.mini.redis.test;

import com.mini.redis.pubsub.PubSubManager;
import com.mini.redis.server.RedisClient;
import com.mini.redis.protocol.RespMessage;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Phase 5 Test - Publish/Subscribe System
 *
 * Tests for PubSub functionality
 *
 * Interview points covered:
 * 1. Observer pattern implementation
 * 2. Message broadcasting
 * 3. Pattern matching
 * 4. Concurrent subscribers
 * 5. Channel isolation
 *
 * @author Mini Redis
 */
public class Phase5PubSubTest {

    private PubSubManager pubSubManager;
    private RedisClient client1;
    private RedisClient client2;
    private RedisClient client3;
    private Channel channel1;
    private Channel channel2;
    private Channel channel3;

    @Before
    public void setUp() {
        // Get PubSub manager instance
        pubSubManager = PubSubManager.getInstance();
        pubSubManager.clearAll();

        // Create mock channels
        channel1 = mock(Channel.class);
        channel2 = mock(Channel.class);
        channel3 = mock(Channel.class);

        when(channel1.isActive()).thenReturn(true);
        when(channel2.isActive()).thenReturn(true);
        when(channel3.isActive()).thenReturn(true);

        // Create mock clients
        client1 = mock(RedisClient.class);
        client2 = mock(RedisClient.class);
        client3 = mock(RedisClient.class);

        when(client1.getId()).thenReturn(1);
        when(client2.getId()).thenReturn(2);
        when(client3.getId()).thenReturn(3);

        when(client1.getChannel()).thenReturn(channel1);
        when(client2.getChannel()).thenReturn(channel2);
        when(client3.getChannel()).thenReturn(channel3);
    }

    /**
     * Test basic subscribe and publish
     */
    @Test
    public void testBasicPubSub() {
        // Subscribe client1 to channel "news"
        int count = pubSubManager.subscribe(client1, "news");
        assertEquals("Should subscribe to 1 channel", 1, count);

        // Verify subscription confirmation was sent
        verify(channel1, times(1)).writeAndFlush(any(RespMessage.Array.class));

        // Publish a message
        int receivers = pubSubManager.publish("news", "Hello World");
        assertEquals("Should have 1 receiver", 1, receivers);

        // Verify message was sent to subscriber
        verify(channel1, times(2)).writeAndFlush(any(RespMessage.Array.class));
    }

    /**
     * Test multiple subscribers to same channel
     */
    @Test
    public void testMultipleSubscribers() {
        // Subscribe multiple clients to same channel
        pubSubManager.subscribe(client1, "sports");
        pubSubManager.subscribe(client2, "sports");
        pubSubManager.subscribe(client3, "sports");

        // Publish message
        int receivers = pubSubManager.publish("sports", "Game Score: 3-2");
        assertEquals("Should have 3 receivers", 3, receivers);

        // Verify all subscribers received the message
        verify(channel1, times(2)).writeAndFlush(any(RespMessage.Array.class));
        verify(channel2, times(2)).writeAndFlush(any(RespMessage.Array.class));
        verify(channel3, times(2)).writeAndFlush(any(RespMessage.Array.class));
    }

    /**
     * Test pattern subscription
     */
    @Test
    public void testPatternSubscription() {
        // Subscribe to pattern
        int count = pubSubManager.psubscribe(client1, "news.*");
        assertEquals("Should subscribe to 1 pattern", 1, count);

        // Publish to matching channels
        int receivers1 = pubSubManager.publish("news.sports", "Sports news");
        int receivers2 = pubSubManager.publish("news.tech", "Tech news");
        int receivers3 = pubSubManager.publish("weather", "Sunny");

        assertEquals("Should match pattern", 1, receivers1);
        assertEquals("Should match pattern", 1, receivers2);
        assertEquals("Should not match pattern", 0, receivers3);
    }

    /**
     * Test unsubscribe
     */
    @Test
    public void testUnsubscribe() {
        // Subscribe to multiple channels
        pubSubManager.subscribe(client1, "ch1", "ch2", "ch3");
        assertTrue("Should be in pubsub mode", pubSubManager.isInPubSubMode(client1));

        // Unsubscribe from specific channel
        int count = pubSubManager.unsubscribe(client1, "ch2");
        assertEquals("Should unsubscribe from 1 channel", 1, count);

        // Publish to channels
        pubSubManager.publish("ch1", "msg1");
        pubSubManager.publish("ch2", "msg2");
        pubSubManager.publish("ch3", "msg3");

        // Verify client1 didn't receive message for ch2
        // Total writeAndFlush calls: 3 (initial subscribe) + 1 (unsubscribe) + 2 (messages for ch1 and ch3)
        verify(channel1, times(6)).writeAndFlush(any(RespMessage.Array.class));
    }

    /**
     * Test unsubscribe all
     */
    @Test
    public void testUnsubscribeAll() {
        // Subscribe to multiple channels
        pubSubManager.subscribe(client1, "ch1", "ch2", "ch3");

        // Unsubscribe from all (empty array)
        int count = pubSubManager.unsubscribe(client1);
        assertEquals("Should unsubscribe from all 3 channels", 3, count);

        assertFalse("Should not be in pubsub mode", pubSubManager.isInPubSubMode(client1));
    }

    /**
     * Test pattern unsubscribe
     */
    @Test
    public void testPUnsubscribe() {
        // Subscribe to patterns
        pubSubManager.psubscribe(client1, "news.*", "sports.*");

        // Unsubscribe from one pattern
        int count = pubSubManager.punsubscribe(client1, "news.*");
        assertEquals("Should unsubscribe from 1 pattern", 1, count);

        // Publish messages
        pubSubManager.publish("news.tech", "Tech news");
        pubSubManager.publish("sports.football", "Football news");

        // Verify only sports message was received
        // Total: 2 (psubscribe) + 1 (punsubscribe) + 1 (sports message)
        verify(channel1, times(4)).writeAndFlush(any(RespMessage.Array.class));
    }

    /**
     * Test mixed channel and pattern subscriptions
     */
    @Test
    public void testMixedSubscriptions() {
        // Subscribe to both channels and patterns
        pubSubManager.subscribe(client1, "exact");
        pubSubManager.psubscribe(client1, "prefix.*");

        // Publish messages
        int r1 = pubSubManager.publish("exact", "Exact match");
        int r2 = pubSubManager.publish("prefix.test", "Pattern match");
        int r3 = pubSubManager.publish("other", "No match");

        assertEquals("Exact channel match", 1, r1);
        assertEquals("Pattern match", 1, r2);
        assertEquals("No match", 0, r3);
    }

    /**
     * Test client cleanup on disconnect
     */
    @Test
    public void testClientCleanup() {
        // Subscribe client to channels and patterns
        pubSubManager.subscribe(client1, "ch1", "ch2");
        pubSubManager.psubscribe(client1, "pat.*");

        assertTrue("Should be in pubsub mode", pubSubManager.isInPubSubMode(client1));

        // Simulate client disconnect
        pubSubManager.removeClient(client1);

        assertFalse("Should not be in pubsub mode after cleanup",
            pubSubManager.isInPubSubMode(client1));

        // Publish should have no receivers
        int receivers = pubSubManager.publish("ch1", "test");
        assertEquals("Should have no receivers after cleanup", 0, receivers);
    }

    /**
     * Test subscription count
     */
    @Test
    public void testSubscriptionCount() {
        assertEquals("Should have 0 subscriptions initially", 0,
            pubSubManager.getSubscriptionCount(client1));

        pubSubManager.subscribe(client1, "ch1", "ch2", "ch3");
        assertEquals("Should have 3 channel subscriptions", 3,
            pubSubManager.getSubscriptionCount(client1));

        pubSubManager.psubscribe(client1, "pat1.*", "pat2.*");
        assertEquals("Should have 5 total subscriptions", 5,
            pubSubManager.getSubscriptionCount(client1));

        pubSubManager.unsubscribe(client1, "ch1");
        assertEquals("Should have 4 subscriptions after unsubscribe", 4,
            pubSubManager.getSubscriptionCount(client1));
    }

    /**
     * Test concurrent publish/subscribe
     */
    @Test
    public void testConcurrentPubSub() throws InterruptedException {
        String channel = "concurrent";
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(3);
        AtomicInteger totalMessages = new AtomicInteger(0);

        // Thread 1: Publisher
        Thread publisher = new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < 100; i++) {
                    pubSubManager.publish(channel, "Message " + i);
                    totalMessages.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        });

        // Thread 2: Subscriber
        Thread subscriber1 = new Thread(() -> {
            try {
                startLatch.await();
                pubSubManager.subscribe(client1, channel);
                Thread.sleep(50); // Simulate processing
                pubSubManager.unsubscribe(client1, channel);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        });

        // Thread 3: Another subscriber
        Thread subscriber2 = new Thread(() -> {
            try {
                startLatch.await();
                Thread.sleep(25); // Join late
                pubSubManager.subscribe(client2, channel);
                Thread.sleep(25);
                pubSubManager.unsubscribe(client2, channel);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        });

        // Start all threads
        publisher.start();
        subscriber1.start();
        subscriber2.start();

        // Release all threads
        startLatch.countDown();

        // Wait for completion
        assertTrue("Should complete within 5 seconds",
            endLatch.await(5, TimeUnit.SECONDS));

        assertEquals("Should publish 100 messages", 100, totalMessages.get());
    }

    /**
     * Test pattern matching with wildcards
     */
    @Test
    public void testPatternWildcards() {
        // Test * wildcard (matches any sequence)
        pubSubManager.psubscribe(client1, "h*llo");
        assertEquals(1, pubSubManager.publish("hello", "test"));
        assertEquals(1, pubSubManager.publish("hallo", "test"));
        assertEquals(1, pubSubManager.publish("hxyzllo", "test"));

        // Test ? wildcard (matches single character)
        pubSubManager.psubscribe(client2, "h?llo");
        assertEquals(1, pubSubManager.publish("hallo", "test")); // client1 also receives
        assertEquals(0, pubSubManager.publish("hllo", "test")); // too short
        assertEquals(0, pubSubManager.publish("hxxllo", "test")); // too long

        // Test complex pattern
        pubSubManager.psubscribe(client3, "user:*:event:?");
        assertEquals(1, pubSubManager.publish("user:123:event:a", "test"));
        assertEquals(1, pubSubManager.publish("user:456:event:b", "test"));
        assertEquals(0, pubSubManager.publish("user:789:event:ab", "test")); // ? matches only one char
    }

    /**
     * Test statistics
     */
    @Test
    public void testStatistics() {
        var initialStats = pubSubManager.getStats();
        assertEquals(0, initialStats.get("total_channels"));
        assertEquals(0L, initialStats.get("total_messages"));

        // Add subscriptions
        pubSubManager.subscribe(client1, "ch1", "ch2");
        pubSubManager.psubscribe(client1, "pat.*");

        // Publish messages
        pubSubManager.publish("ch1", "msg1");
        pubSubManager.publish("ch2", "msg2");
        pubSubManager.publish("pat.test", "msg3");

        var stats = pubSubManager.getStats();
        assertEquals(2, stats.get("total_channels"));
        assertEquals(1, stats.get("total_patterns"));
        assertEquals(3L, stats.get("total_messages"));
        assertTrue((Long)stats.get("total_subscriptions") > 0);
    }
}