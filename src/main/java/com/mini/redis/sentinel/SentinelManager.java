package com.mini.redis.sentinel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis Sentinel Manager (Simplified)
 *
 * Interview points covered:
 * 1. Master monitoring and health checks
 * 2. Automatic failover
 * 3. Quorum-based decision making
 * 4. Configuration management
 * 5. Notification system
 *
 * @author Mini Redis
 */
public class SentinelManager {

    private static final Logger logger = LoggerFactory.getLogger(SentinelManager.class);

    // Singleton instance
    private static volatile SentinelManager instance;

    // Monitored masters
    private final ConcurrentHashMap<String, MasterInfo> masters = new ConcurrentHashMap<>();

    // Health check scheduler
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // Sentinel configuration
    private int quorum = 2; // Minimum sentinels to agree for failover
    private int failoverTimeout = 30000; // 30 seconds
    private int downAfterMillis = 5000; // Mark down after 5 seconds

    /**
     * Master information
     */
    public static class MasterInfo {
        public final String name;
        public String host;
        public int port;
        public boolean isDown = false;
        public long lastPingTime = System.currentTimeMillis();
        public final AtomicInteger downVotes = new AtomicInteger(0);

        public MasterInfo(String name, String host, int port) {
            this.name = name;
            this.host = host;
            this.port = port;
        }
    }

    private SentinelManager() {
        startHealthChecks();
    }

    /**
     * Get singleton instance
     */
    public static SentinelManager getInstance() {
        if (instance == null) {
            synchronized (SentinelManager.class) {
                if (instance == null) {
                    instance = new SentinelManager();
                }
            }
        }
        return instance;
    }

    /**
     * Monitor a master
     */
    public void monitorMaster(String masterName, String host, int port) {
        MasterInfo master = new MasterInfo(masterName, host, port);
        masters.put(masterName, master);
        logger.info("Monitoring master: {} at {}:{}", masterName, host, port);
    }

    /**
     * Remove master from monitoring
     */
    public void removeMonitor(String masterName) {
        masters.remove(masterName);
        logger.info("Stopped monitoring master: {}", masterName);
    }

    /**
     * Start health check scheduler
     */
    private void startHealthChecks() {
        // Ping masters every second
        scheduler.scheduleWithFixedDelay(this::checkMasters, 1, 1, TimeUnit.SECONDS);

        // Check for failover every 5 seconds
        scheduler.scheduleWithFixedDelay(this::checkFailover, 5, 5, TimeUnit.SECONDS);
    }

    /**
     * Check all masters health
     */
    private void checkMasters() {
        for (MasterInfo master : masters.values()) {
            checkMasterHealth(master);
        }
    }

    /**
     * Check individual master health
     */
    private void checkMasterHealth(MasterInfo master) {
        // Simplified: simulate ping
        boolean pingSuccess = simulatePing(master);

        if (pingSuccess) {
            master.lastPingTime = System.currentTimeMillis();
            if (master.isDown) {
                master.isDown = false;
                master.downVotes.set(0);
                logger.info("Master {} is back online", master.name);
            }
        } else {
            long downTime = System.currentTimeMillis() - master.lastPingTime;
            if (downTime > downAfterMillis && !master.isDown) {
                markMasterDown(master);
            }
        }
    }

    /**
     * Simulate ping (in real implementation would actually ping)
     */
    private boolean simulatePing(MasterInfo master) {
        // Simplified: always return true for demo
        return true;
    }

    /**
     * Mark master as down
     */
    private void markMasterDown(MasterInfo master) {
        master.isDown = true;
        master.downVotes.incrementAndGet();
        logger.warn("Master {} marked as DOWN", master.name);

        // Send notification
        sendNotification("master-down", master.name);
    }

    /**
     * Check if failover is needed
     */
    private void checkFailover() {
        for (MasterInfo master : masters.values()) {
            if (master.isDown && master.downVotes.get() >= quorum) {
                performFailover(master);
            }
        }
    }

    /**
     * Perform failover
     */
    private void performFailover(MasterInfo master) {
        logger.info("Starting failover for master: {}", master.name);

        // Steps in real implementation:
        // 1. Select best slave
        // 2. Promote slave to master
        // 3. Reconfigure other slaves
        // 4. Update configuration
        // 5. Notify clients

        // Simplified: just log and reset
        logger.info("Failover completed for master: {} (simulated)", master.name);
        master.isDown = false;
        master.downVotes.set(0);
        master.lastPingTime = System.currentTimeMillis();

        // Send notification
        sendNotification("failover-completed", master.name);
    }

    /**
     * Send notification
     */
    private void sendNotification(String type, String message) {
        logger.info("NOTIFICATION: {} - {}", type, message);
        // In real implementation, would send to subscribers
    }

    /**
     * Get sentinel info
     */
    public String getInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("sentinel_masters:").append(masters.size()).append("\n");

        for (MasterInfo master : masters.values()) {
            sb.append("master:").append(master.name)
              .append(",status:").append(master.isDown ? "down" : "ok")
              .append(",address:").append(master.host).append(":").append(master.port)
              .append(",down-votes:").append(master.downVotes.get())
              .append("\n");
        }

        sb.append("sentinel_quorum:").append(quorum).append("\n");
        sb.append("sentinel_failover_timeout:").append(failoverTimeout).append("\n");

        return sb.toString();
    }

    /**
     * Shutdown sentinel
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("Sentinel shutdown complete");
    }

    // Configuration methods
    public void setQuorum(int quorum) {
        this.quorum = quorum;
    }

    public int getQuorum() {
        return quorum;
    }

    public void setDownAfterMillis(int millis) {
        this.downAfterMillis = millis;
    }

    public void setFailoverTimeout(int timeout) {
        this.failoverTimeout = timeout;
    }
}