package com.mini.redis.storage.impl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Redis Sorted Set (ZSet) 数据结构实现
 *
 * 使用跳表（SkipList）+ 哈希表实现
 * - 跳表：按分数排序，支持范围查询
 * - 哈希表：O(1) 查找成员的分数
 *
 * 面试知识点：
 * 1. 跳表（Skip List）的原理和实现
 * 2. 为什么 Redis 选择跳表而不是红黑树
 * 3. 组合数据结构的设计
 * 4. 并发控制
 *
 * @author Mini Redis
 */
public class RedisZSet {

    /**
     * ZSet 成员节点
     * 包含成员名称和分数
     */
    private static class ZSetNode implements Comparable<ZSetNode> {
        final String member;
        final double score;

        ZSetNode(String member, double score) {
            this.member = member;
            this.score = score;
        }

        @Override
        public int compareTo(ZSetNode other) {
            // 先按分数排序
            int scoreCompare = Double.compare(this.score, other.score);
            if (scoreCompare != 0) {
                return scoreCompare;
            }
            // 分数相同时按成员名字典序排序
            return this.member.compareTo(other.member);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            ZSetNode node = (ZSetNode) obj;
            return Double.compare(node.score, score) == 0 &&
                   member.equals(node.member);
        }

        @Override
        public int hashCode() {
            return Objects.hash(member, score);
        }
    }

    /**
     * 跳表存储（按分数排序）
     */
    private final ConcurrentSkipListSet<ZSetNode> skipList;

    /**
     * 哈希表存储（成员 -> 分数）
     */
    private final ConcurrentHashMap<String, Double> scoreMap;

    /**
     * 读写锁
     */
    private final ReadWriteLock lock;

    /**
     * 构造函数
     */
    public RedisZSet() {
        this.skipList = new ConcurrentSkipListSet<>();
        this.scoreMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * 添加成员及其分数
     *
     * @param score 分数
     * @param member 成员
     * @return 新增返回 1，更新返回 0
     */
    public int zadd(double score, String member) {
        lock.writeLock().lock();
        try {
            Double oldScore = scoreMap.get(member);

            if (oldScore != null) {
                // 成员已存在，更新分数
                if (Double.compare(oldScore, score) != 0) {
                    // 分数变化，需要更新跳表
                    skipList.remove(new ZSetNode(member, oldScore));
                    skipList.add(new ZSetNode(member, score));
                    scoreMap.put(member, score);
                }
                return 0;  // 更新操作
            } else {
                // 新增成员
                skipList.add(new ZSetNode(member, score));
                scoreMap.put(member, score);
                return 1;  // 新增操作
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 批量添加成员
     *
     * @param memberScores 成员-分数对
     * @return 新增的成员数量
     */
    public int zadd(Map<String, Double> memberScores) {
        int added = 0;
        for (Map.Entry<String, Double> entry : memberScores.entrySet()) {
            added += zadd(entry.getValue(), entry.getKey());
        }
        return added;
    }

    /**
     * 移除成员
     *
     * @param members 要移除的成员
     * @return 成功移除的数量
     */
    public int zrem(String... members) {
        lock.writeLock().lock();
        try {
            int removed = 0;
            for (String member : members) {
                Double score = scoreMap.remove(member);
                if (score != null) {
                    skipList.remove(new ZSetNode(member, score));
                    removed++;
                }
            }
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取成员的分数
     *
     * @param member 成员
     * @return 分数，如果成员不存在返回 null
     */
    public Double zscore(String member) {
        lock.readLock().lock();
        try {
            return scoreMap.get(member);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 增加成员的分数
     *
     * @param member 成员
     * @param increment 增量
     * @return 新分数
     */
    public double zincrby(String member, double increment) {
        lock.writeLock().lock();
        try {
            Double oldScore = scoreMap.get(member);
            double newScore = (oldScore != null ? oldScore : 0.0) + increment;

            if (oldScore != null) {
                skipList.remove(new ZSetNode(member, oldScore));
            }

            skipList.add(new ZSetNode(member, newScore));
            scoreMap.put(member, newScore);

            return newScore;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取成员数量
     *
     * @return 成员数量
     */
    public int zcard() {
        lock.readLock().lock();
        try {
            return scoreMap.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 按索引范围获取成员（从低到高）
     *
     * @param start 起始索引（0开始，可以为负数）
     * @param stop 结束索引（包含，可以为负数）
     * @param withScores 是否返回分数
     * @return 成员列表
     */
    public List<String> zrange(long start, long stop, boolean withScores) {
        lock.readLock().lock();
        try {
            int size = skipList.size();
            if (size == 0) {
                return new ArrayList<>();
            }

            // 规范化索引
            int actualStart = normalizeIndex((int) start, size);
            int actualStop = normalizeIndex((int) stop, size);

            if (actualStart > actualStop || actualStart >= size || actualStop < 0) {
                return new ArrayList<>();
            }

            actualStart = Math.max(0, actualStart);
            actualStop = Math.min(size - 1, actualStop);

            List<String> result = new ArrayList<>();
            int index = 0;

            for (ZSetNode node : skipList) {
                if (index > actualStop) {
                    break;
                }
                if (index >= actualStart) {
                    result.add(node.member);
                    if (withScores) {
                        result.add(String.valueOf(node.score));
                    }
                }
                index++;
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 按索引范围获取成员（从高到低）
     *
     * @param start 起始索引
     * @param stop 结束索引
     * @param withScores 是否返回分数
     * @return 成员列表
     */
    public List<String> zrevrange(long start, long stop, boolean withScores) {
        lock.readLock().lock();
        try {
            // 获取正序结果
            List<ZSetNode> nodes = new ArrayList<>(skipList);
            Collections.reverse(nodes);

            int size = nodes.size();
            if (size == 0) {
                return new ArrayList<>();
            }

            // 规范化索引
            int actualStart = normalizeIndex((int) start, size);
            int actualStop = normalizeIndex((int) stop, size);

            if (actualStart > actualStop || actualStart >= size || actualStop < 0) {
                return new ArrayList<>();
            }

            actualStart = Math.max(0, actualStart);
            actualStop = Math.min(size - 1, actualStop);

            List<String> result = new ArrayList<>();
            for (int i = actualStart; i <= actualStop; i++) {
                ZSetNode node = nodes.get(i);
                result.add(node.member);
                if (withScores) {
                    result.add(String.valueOf(node.score));
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 按分数范围获取成员
     *
     * @param min 最小分数
     * @param max 最大分数
     * @param withScores 是否返回分数
     * @return 成员列表
     */
    public List<String> zrangebyscore(double min, double max, boolean withScores) {
        lock.readLock().lock();
        try {
            List<String> result = new ArrayList<>();

            for (ZSetNode node : skipList) {
                if (node.score < min) {
                    continue;
                }
                if (node.score > max) {
                    break;
                }
                result.add(node.member);
                if (withScores) {
                    result.add(String.valueOf(node.score));
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 统计分数范围内的成员数量
     *
     * @param min 最小分数
     * @param max 最大分数
     * @return 成员数量
     */
    public int zcount(double min, double max) {
        lock.readLock().lock();
        try {
            int count = 0;
            for (ZSetNode node : skipList) {
                if (node.score < min) {
                    continue;
                }
                if (node.score > max) {
                    break;
                }
                count++;
            }
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取成员的排名（从低到高）
     *
     * @param member 成员
     * @return 排名（0开始），如果成员不存在返回 null
     */
    public Integer zrank(String member) {
        lock.readLock().lock();
        try {
            Double score = scoreMap.get(member);
            if (score == null) {
                return null;
            }

            int rank = 0;
            for (ZSetNode node : skipList) {
                if (node.member.equals(member)) {
                    return rank;
                }
                rank++;
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取成员的逆序排名（从高到低）
     *
     * @param member 成员
     * @return 逆序排名（0开始），如果成员不存在返回 null
     */
    public Integer zrevrank(String member) {
        lock.readLock().lock();
        try {
            Integer rank = zrank(member);
            if (rank == null) {
                return null;
            }
            return skipList.size() - 1 - rank;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 规范化索引
     *
     * @param index 原始索引
     * @param size 集合大小
     * @return 规范化后的索引
     */
    private int normalizeIndex(int index, int size) {
        if (index < 0) {
            return size + index;
        }
        return index;
    }

    /**
     * 检查是否为空
     *
     * @return 如果为空返回 true
     */
    public boolean isEmpty() {
        return scoreMap.isEmpty();
    }

    /**
     * 获取大小
     *
     * @return 有序集合大小
     */
    public int size() {
        return scoreMap.size();
    }

    /**
     * 清空有序集合
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            skipList.clear();
            scoreMap.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}