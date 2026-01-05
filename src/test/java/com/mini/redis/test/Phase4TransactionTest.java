package com.mini.redis.test;

import com.mini.redis.server.RedisClient;
import com.mini.redis.transaction.TransactionManager;
import com.mini.redis.protocol.RespMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Phase 4 Test - Transaction System
 *
 * Tests for MULTI/EXEC/DISCARD/WATCH/UNWATCH commands
 *
 * Interview points covered:
 * 1. ACID properties (Redis guarantees Isolation and Consistency)
 * 2. Command queuing mechanism
 * 3. Optimistic locking (WATCH)
 * 4. CAS operations
 * 5. Transaction atomicity
 * 6. Concurrency control
 *
 * @author Mini Redis
 */
public class Phase4TransactionTest {

    private TransactionManager transactionManager;
    private RedisClient client1;
    private RedisClient client2;

    @Before
    public void setUp() {
        // Get transaction manager instance
        transactionManager = TransactionManager.getInstance();
        transactionManager.clearAll();

        // Create mock clients
        client1 = mock(RedisClient.class);
        client2 = mock(RedisClient.class);

        when(client1.getId()).thenReturn(1);
        when(client2.getId()).thenReturn(2);
    }

    /**
     * Test basic MULTI/EXEC transaction
     */
    @Test
    public void testBasicTransaction() {
        // Start transaction
        transactionManager.multi(client1);

        TransactionManager.TransactionContext context = transactionManager.getContext(client1);
        assertTrue("Should be in transaction", context.isInTransaction());
        assertEquals("Transaction state should be QUEUING",
            TransactionManager.TransactionState.QUEUING, context.getState());

        // Queue commands
        RespMessage cmd1 = createCommand("SET", "key1", "value1");
        RespMessage cmd2 = createCommand("GET", "key1");
        RespMessage cmd3 = createCommand("INCR", "counter");

        transactionManager.enqueue(client1, cmd1);
        transactionManager.enqueue(client1, cmd2);
        transactionManager.enqueue(client1, cmd3);

        assertEquals("Should have 3 queued commands", 3, context.getQueueSize());

        // Execute transaction
        List<TransactionManager.QueuedCommand> commands = transactionManager.exec(client1);

        assertNotNull("Commands should not be null", commands);
        assertEquals("Should execute 3 commands", 3, commands.size());
        assertFalse("Should not be in transaction after EXEC", context.isInTransaction());
        assertEquals("Transaction state should be NONE",
            TransactionManager.TransactionState.NONE, context.getState());
    }

    /**
     * Test DISCARD command
     */
    @Test
    public void testDiscardTransaction() {
        // Start transaction
        transactionManager.multi(client1);

        // Queue commands
        RespMessage cmd1 = createCommand("SET", "key1", "value1");
        RespMessage cmd2 = createCommand("GET", "key1");

        transactionManager.enqueue(client1, cmd1);
        transactionManager.enqueue(client1, cmd2);

        TransactionManager.TransactionContext context = transactionManager.getContext(client1);
        assertEquals("Should have 2 queued commands", 2, context.getQueueSize());

        // Discard transaction
        transactionManager.discard(client1);

        assertFalse("Should not be in transaction after DISCARD", context.isInTransaction());
        assertEquals("Should have 0 queued commands after DISCARD", 0, context.getQueueSize());
        assertEquals("Transaction state should be NONE",
            TransactionManager.TransactionState.NONE, context.getState());
    }

    /**
     * Test WATCH mechanism
     */
    @Test
    public void testWatchMechanism() {
        String watchedKey = "watched_key";

        // Client 1 watches a key
        transactionManager.watch(client1, watchedKey);

        TransactionManager.TransactionContext context1 = transactionManager.getContext(client1);
        assertTrue("Key should be watched", context1.getWatchedKeys().contains(watchedKey));

        // Client 1 starts transaction
        transactionManager.multi(client1);
        transactionManager.enqueue(client1, createCommand("SET", watchedKey, "new_value"));

        // Client 2 modifies the watched key (simulating concurrent modification)
        transactionManager.notifyKeyModified(watchedKey);

        // Client 1 tries to execute transaction
        List<TransactionManager.QueuedCommand> commands = transactionManager.exec(client1);

        assertNull("Transaction should be aborted due to WATCH", commands);
        assertFalse("Should not be in transaction", context1.isInTransaction());
    }

    /**
     * Test UNWATCH command
     */
    @Test
    public void testUnwatchCommand() {
        String watchedKey = "watched_key";

        // Watch a key
        transactionManager.watch(client1, watchedKey);

        TransactionManager.TransactionContext context = transactionManager.getContext(client1);
        assertTrue("Key should be watched", context.getWatchedKeys().contains(watchedKey));

        // Unwatch all keys
        transactionManager.unwatch(client1);

        assertTrue("Should have no watched keys", context.getWatchedKeys().isEmpty());
    }

    /**
     * Test concurrent transactions (different clients)
     */
    @Test
    public void testConcurrentTransactions() throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Client 1 transaction
        executor.submit(() -> {
            try {
                startLatch.await();

                transactionManager.multi(client1);
                transactionManager.enqueue(client1, createCommand("SET", "key1", "value1"));
                transactionManager.enqueue(client1, createCommand("SET", "key2", "value2"));

                List<TransactionManager.QueuedCommand> commands = transactionManager.exec(client1);
                if (commands != null && commands.size() == 2) {
                    successCount.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        });

        // Client 2 transaction
        executor.submit(() -> {
            try {
                startLatch.await();

                transactionManager.multi(client2);
                transactionManager.enqueue(client2, createCommand("SET", "key3", "value3"));
                transactionManager.enqueue(client2, createCommand("SET", "key4", "value4"));

                List<TransactionManager.QueuedCommand> commands = transactionManager.exec(client2);
                if (commands != null && commands.size() == 2) {
                    successCount.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        });

        // Start both transactions
        startLatch.countDown();

        // Wait for completion
        assertTrue("Transactions should complete", endLatch.await(5, TimeUnit.SECONDS));
        assertEquals("Both transactions should succeed", 2, successCount.get());

        executor.shutdown();
    }

    /**
     * Test optimistic locking with concurrent modifications
     */
    @Test
    public void testOptimisticLocking() throws InterruptedException {
        String sharedKey = "shared_counter";
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(2);
        AtomicInteger successCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Client 1 tries to increment counter
        executor.submit(() -> {
            try {
                startLatch.await();

                // Watch the key
                transactionManager.watch(client1, sharedKey);

                // Start transaction
                transactionManager.multi(client1);
                transactionManager.enqueue(client1, createCommand("INCR", sharedKey));

                // Simulate some processing time
                Thread.sleep(100);

                // Try to execute
                List<TransactionManager.QueuedCommand> commands = transactionManager.exec(client1);
                if (commands != null) {
                    successCount.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        });

        // Client 2 modifies the key
        executor.submit(() -> {
            try {
                startLatch.await();

                // Wait a bit to ensure client1 has watched the key
                Thread.sleep(50);

                // Modify the watched key
                transactionManager.notifyKeyModified(sharedKey);

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                endLatch.countDown();
            }
        });

        // Start both operations
        startLatch.countDown();

        // Wait for completion
        assertTrue("Operations should complete", endLatch.await(5, TimeUnit.SECONDS));

        // Only one should succeed due to optimistic locking
        assertEquals("Client1 transaction should be aborted", 0, successCount.get());

        executor.shutdown();
    }

    /**
     * Test nested MULTI calls (should fail)
     */
    @Test(expected = IllegalStateException.class)
    public void testNestedMulti() {
        transactionManager.multi(client1);
        transactionManager.multi(client1); // Should throw exception
    }

    /**
     * Test EXEC without MULTI
     */
    @Test(expected = IllegalStateException.class)
    public void testExecWithoutMulti() {
        transactionManager.exec(client1); // Should throw exception
    }

    /**
     * Test DISCARD without MULTI
     */
    @Test(expected = IllegalStateException.class)
    public void testDiscardWithoutMulti() {
        transactionManager.discard(client1); // Should throw exception
    }

    /**
     * Test transaction isolation
     */
    @Test
    public void testTransactionIsolation() {
        // Start transactions for both clients
        transactionManager.multi(client1);
        transactionManager.multi(client2);

        // Queue commands for client1
        transactionManager.enqueue(client1, createCommand("SET", "key1", "value1"));

        // Queue commands for client2
        transactionManager.enqueue(client2, createCommand("SET", "key2", "value2"));

        TransactionManager.TransactionContext context1 = transactionManager.getContext(client1);
        TransactionManager.TransactionContext context2 = transactionManager.getContext(client2);

        // Each client should have their own queued commands
        assertEquals("Client1 should have 1 command", 1, context1.getQueueSize());
        assertEquals("Client2 should have 1 command", 1, context2.getQueueSize());

        // Execute client1's transaction
        List<TransactionManager.QueuedCommand> commands1 = transactionManager.exec(client1);
        assertNotNull("Client1 commands should not be null", commands1);
        assertEquals("Client1 should execute 1 command", 1, commands1.size());

        // Client2 should still be in transaction
        assertTrue("Client2 should still be in transaction", context2.isInTransaction());

        // Execute client2's transaction
        List<TransactionManager.QueuedCommand> commands2 = transactionManager.exec(client2);
        assertNotNull("Client2 commands should not be null", commands2);
        assertEquals("Client2 should execute 1 command", 1, commands2.size());
    }

    /**
     * Test global statistics
     */
    @Test
    public void testGlobalStatistics() {
        // Start transactions
        transactionManager.multi(client1);
        transactionManager.multi(client2);

        // Watch some keys
        transactionManager.watch(client1, "key1");
        transactionManager.watch(client1, "key2");
        transactionManager.watch(client2, "key3");

        // Get statistics
        var stats = transactionManager.getStats();

        assertEquals("Should have 2 active transactions", 2L,
            stats.get("active_transactions"));
        assertEquals("Should have 2 total clients", 2,
            stats.get("total_clients"));
        assertEquals("Should have 3 watched keys total", 3,
            stats.get("watched_keys_count"));
    }

    /**
     * Helper method to create a command message
     */
    private RespMessage createCommand(String cmd, String... args) {
        List<RespMessage> elements = new java.util.ArrayList<>();
        elements.add(new RespMessage.BulkString(cmd));
        for (String arg : args) {
            elements.add(new RespMessage.BulkString(arg));
        }
        return new RespMessage.Array(elements);
    }
}