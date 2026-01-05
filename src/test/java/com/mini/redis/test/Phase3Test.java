package com.mini.redis.test;

import com.mini.redis.storage.impl.RedisList;
import com.mini.redis.storage.impl.RedisHash;
import com.mini.redis.storage.impl.RedisSet;
import com.mini.redis.storage.impl.RedisZSet;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Phase 3 Test - Complex Data Types
 *
 * Tests for List, Hash, Set, and ZSet data structures
 *
 * @author Mini Redis
 */
public class Phase3Test {

    /**
     * Test List operations
     */
    @Test
    public void testListOperations() {
        RedisList list = new RedisList();

        // Test lpush
        assertEquals(1, list.lpush("item1"));
        assertEquals(2, list.lpush("item2"));

        // Test rpush
        assertEquals(3, list.rpush("item3"));

        // Test llen
        assertEquals(3, list.llen());

        // Test lpop
        assertEquals("item2", list.lpop());
        assertEquals(2, list.llen());

        // Test rpop
        assertEquals("item3", list.rpop());
        assertEquals(1, list.llen());

        // Test lrange
        list.rpush("item2", "item3", "item4");
        List<String> range = list.lrange(0, -1);
        assertEquals(4, range.size());
        assertEquals("item1", range.get(0));
        assertEquals("item4", range.get(3));

        // Test negative indices
        range = list.lrange(-2, -1);
        assertEquals(2, range.size());
        assertEquals("item3", range.get(0));
        assertEquals("item4", range.get(1));
    }

    /**
     * Test Hash operations
     */
    @Test
    public void testHashOperations() {
        RedisHash hash = new RedisHash();

        // Test hset
        assertEquals(1, hash.hset("field1", "value1"));
        assertEquals(0, hash.hset("field1", "updated1"));  // Update returns 0

        // Test hget
        assertEquals("updated1", hash.hget("field1"));
        assertNull(hash.hget("nonexistent"));

        // Test hdel
        hash.hset("field2", "value2");
        hash.hset("field3", "value3");
        assertEquals(2, hash.hdel("field2", "field3"));
        assertEquals(0, hash.hdel("nonexistent"));

        // Test hlen
        assertEquals(1, hash.hlen());

        // Test hgetall
        hash.hset("field2", "value2");
        Map<String, String> all = hash.hgetall();
        assertEquals(2, all.size());
        assertEquals("updated1", all.get("field1"));
        assertEquals("value2", all.get("field2"));

        // Test hexists
        assertTrue(hash.hexists("field1"));
        assertFalse(hash.hexists("nonexistent"));

        // Test hincrby
        hash.hset("counter", "10");
        assertEquals(15, hash.hincrby("counter", 5));
        assertEquals(10, hash.hincrby("counter", -5));
    }

    /**
     * Test Set operations
     */
    @Test
    public void testSetOperations() {
        RedisSet set = new RedisSet();

        // Test sadd
        assertEquals(1, set.sadd("member1"));
        assertEquals(0, set.sadd("member1"));  // Duplicate
        assertEquals(2, set.sadd("member2", "member3"));

        // Test scard
        assertEquals(3, set.scard());

        // Test sismember
        assertTrue(set.sismember("member1"));
        assertFalse(set.sismember("nonexistent"));

        // Test smembers
        Set<String> members = set.smembers();
        assertEquals(3, members.size());
        assertTrue(members.contains("member1"));
        assertTrue(members.contains("member2"));
        assertTrue(members.contains("member3"));

        // Test srem
        assertEquals(1, set.srem("member1"));
        assertEquals(0, set.srem("nonexistent"));
        assertEquals(2, set.scard());

        // Test set operations
        RedisSet set2 = new RedisSet();
        set2.sadd("member2", "member4");

        // Test sinter (intersection)
        Set<String> inter = set.sinter(set2);
        assertEquals(1, inter.size());
        assertTrue(inter.contains("member2"));

        // Test sunion (union)
        Set<String> union = set.sunion(set2);
        assertEquals(3, union.size());
        assertTrue(union.contains("member2"));
        assertTrue(union.contains("member3"));
        assertTrue(union.contains("member4"));

        // Test sdiff (difference)
        Set<String> diff = set.sdiff(set2);
        assertEquals(1, diff.size());
        assertTrue(diff.contains("member3"));
    }

    /**
     * Test ZSet operations
     */
    @Test
    public void testZSetOperations() {
        RedisZSet zset = new RedisZSet();

        // Test zadd
        assertEquals(1, zset.zadd(1.0, "member1"));
        assertEquals(0, zset.zadd(2.0, "member1"));  // Update
        assertEquals(1, zset.zadd(3.0, "member2"));
        assertEquals(1, zset.zadd(1.5, "member3"));

        // Test zcard
        assertEquals(3, zset.zcard());

        // Test zscore
        assertEquals(Double.valueOf(2.0), zset.zscore("member1"));
        assertEquals(Double.valueOf(3.0), zset.zscore("member2"));
        assertNull(zset.zscore("nonexistent"));

        // Test zrank (ascending)
        assertEquals(Integer.valueOf(1), zset.zrank("member1"));
        assertEquals(Integer.valueOf(0), zset.zrank("member3"));
        assertEquals(Integer.valueOf(2), zset.zrank("member2"));

        // Test zrevrank (descending)
        assertEquals(Integer.valueOf(0), zset.zrevrank("member2"));
        assertEquals(Integer.valueOf(1), zset.zrevrank("member1"));
        assertEquals(Integer.valueOf(2), zset.zrevrank("member3"));

        // Test zrange
        List<String> range = zset.zrange(0, -1, false);
        assertEquals(3, range.size());
        assertEquals("member3", range.get(0));  // score 1.5
        assertEquals("member1", range.get(1));  // score 2.0
        assertEquals("member2", range.get(2));  // score 3.0

        // Test zrange with scores
        range = zset.zrange(0, 1, true);
        assertEquals(4, range.size());
        assertEquals("member3", range.get(0));
        assertEquals("1.5", range.get(1));
        assertEquals("member1", range.get(2));
        assertEquals("2.0", range.get(3));

        // Test zrangebyscore
        range = zset.zrangebyscore(1.0, 2.5, false);
        assertEquals(2, range.size());
        assertEquals("member3", range.get(0));
        assertEquals("member1", range.get(1));

        // Test zincrby
        assertEquals(5.0, zset.zincrby("member2", 2.0), 0.01);
        assertEquals(5.0, zset.zscore("member2"), 0.01);

        // Test zrem
        assertEquals(1, zset.zrem("member1"));
        assertEquals(0, zset.zrem("nonexistent"));
        assertEquals(2, zset.zcard());

        // Test zcount
        assertEquals(1, zset.zcount(1.0, 2.0));
        assertEquals(2, zset.zcount(1.0, 10.0));
    }

    /**
     * Test empty collections
     */
    @Test
    public void testEmptyCollections() {
        // Test empty list
        RedisList list = new RedisList();
        assertTrue(list.isEmpty());
        assertNull(list.lpop());
        assertNull(list.rpop());
        assertTrue(list.lrange(0, -1).isEmpty());

        // Test empty hash
        RedisHash hash = new RedisHash();
        assertTrue(hash.isEmpty());
        assertNull(hash.hget("any"));
        assertTrue(hash.hgetall().isEmpty());

        // Test empty set
        RedisSet set = new RedisSet();
        assertTrue(set.isEmpty());
        assertFalse(set.sismember("any"));
        assertTrue(set.smembers().isEmpty());

        // Test empty zset
        RedisZSet zset = new RedisZSet();
        assertTrue(zset.isEmpty());
        assertNull(zset.zscore("any"));
        assertTrue(zset.zrange(0, -1, false).isEmpty());
    }
}