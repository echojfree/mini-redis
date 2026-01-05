package com.mini.redis.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Master-Slave Replication Manager (Simplified)
 *
 * Interview points covered:
 * 1. Full synchronization (initial sync)
 * 2. Partial synchronization (incremental sync)
 * 3. Replication ID and offset tracking
 * 4. PSYNC protocol
 * 5. Backlog buffer management
 *
 * @author Mini Redis
 */
public class ReplicationManager {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationManager.class);

    // Singleton instance
    private static volatile ReplicationManager instance;

    // Replication role
    public enum Role {
        MASTER,
        SLAVE
    }

    // Current role
    private Role role = Role.MASTER;

    // Master information (for slave)
    private String masterHost;
    private int masterPort;
    private boolean connected = false;

    // Replication ID and offset
    private String replId = generateReplId();
    private AtomicLong replOffset = new AtomicLong(0);

    // Connected slaves (for master)
    private final ConcurrentHashMap<String, SlaveInfo> slaves = new ConcurrentHashMap<>();

    // Replication backlog
    private static final int BACKLOG_SIZE = 1024 * 1024; // 1MB
    private final byte[] backlog = new byte[BACKLOG_SIZE];
    private long backlogOffset = 0;

    /**
     * Slave information
     */
    public static class SlaveInfo {
        public final String id;
        public final InetSocketAddress address;
        public long offset;
        public long lastAckTime;

        public SlaveInfo(String id, InetSocketAddress address) {
            this.id = id;
            this.address = address;
            this.offset = 0;
            this.lastAckTime = System.currentTimeMillis();
        }
    }

    private ReplicationManager() {
    }

    /**
     * Get singleton instance
     */
    public static ReplicationManager getInstance() {
        if (instance == null) {
            synchronized (ReplicationManager.class) {
                if (instance == null) {
                    instance = new ReplicationManager();
                }
            }
        }
        return instance;
    }

    /**
     * Configure as master
     */
    public void becomeMaster() {
        this.role = Role.MASTER;
        this.replId = generateReplId();
        this.replOffset.set(0);
        logger.info("Configured as MASTER with replid: {}", replId);
    }

    /**
     * Configure as slave
     */
    public void becomeSlave(String masterHost, int masterPort) {
        this.role = Role.SLAVE;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.connected = false;
        logger.info("Configured as SLAVE of {}:{}", masterHost, masterPort);
    }

    /**
     * Add slave (for master)
     */
    public void addSlave(String slaveId, InetSocketAddress address) {
        if (role != Role.MASTER) {
            return;
        }

        SlaveInfo slave = new SlaveInfo(slaveId, address);
        slaves.put(slaveId, slave);
        logger.info("Added slave: {} at {}", slaveId, address);
    }

    /**
     * Remove slave (for master)
     */
    public void removeSlave(String slaveId) {
        SlaveInfo removed = slaves.remove(slaveId);
        if (removed != null) {
            logger.info("Removed slave: {}", slaveId);
        }
    }

    /**
     * Update slave offset
     */
    public void updateSlaveOffset(String slaveId, long offset) {
        SlaveInfo slave = slaves.get(slaveId);
        if (slave != null) {
            slave.offset = offset;
            slave.lastAckTime = System.currentTimeMillis();
        }
    }

    /**
     * Propagate command to slaves
     */
    public void propagateCommand(byte[] command) {
        if (role != Role.MASTER) {
            return;
        }

        // Add to backlog
        addToBacklog(command);

        // Send to all slaves
        for (SlaveInfo slave : slaves.values()) {
            // In real implementation, would send via network
            logger.debug("Propagating command to slave {}", slave.id);
        }

        // Update replication offset
        replOffset.addAndGet(command.length);
    }

    /**
     * Add command to backlog
     */
    private void addToBacklog(byte[] command) {
        // Simplified circular buffer implementation
        int pos = (int) (backlogOffset % BACKLOG_SIZE);
        int remaining = BACKLOG_SIZE - pos;

        if (command.length <= remaining) {
            System.arraycopy(command, 0, backlog, pos, command.length);
        } else {
            // Wrap around
            System.arraycopy(command, 0, backlog, pos, remaining);
            System.arraycopy(command, remaining, backlog, 0, command.length - remaining);
        }

        backlogOffset += command.length;
    }

    /**
     * Perform full synchronization
     */
    public void fullSync(String slaveId) {
        logger.info("Starting full sync with slave: {}", slaveId);

        // 1. Send RDB snapshot
        // 2. Send buffered commands during RDB creation
        // 3. Continue streaming commands

        // Simplified: just update slave offset to current
        SlaveInfo slave = slaves.get(slaveId);
        if (slave != null) {
            slave.offset = replOffset.get();
        }
    }

    /**
     * Perform partial synchronization
     */
    public boolean partialSync(String slaveId, String replId, long offset) {
        logger.info("Attempting partial sync with slave: {} at offset {}", slaveId, offset);

        // Check if partial sync is possible
        if (!this.replId.equals(replId)) {
            logger.info("Replication ID mismatch, full sync required");
            return false;
        }

        long currentOffset = replOffset.get();
        long lag = currentOffset - offset;

        if (lag > BACKLOG_SIZE) {
            logger.info("Slave too far behind, full sync required");
            return false;
        }

        // Send backlog from offset to current
        logger.info("Partial sync successful, sending {} bytes", lag);
        return true;
    }

    /**
     * Generate replication ID
     */
    private String generateReplId() {
        return Long.toHexString(System.currentTimeMillis()) +
               Long.toHexString(System.nanoTime());
    }

    /**
     * Get replication info
     */
    public String getInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("role:").append(role.toString().toLowerCase()).append("\n");
        sb.append("repl_id:").append(replId).append("\n");
        sb.append("repl_offset:").append(replOffset.get()).append("\n");

        if (role == Role.MASTER) {
            sb.append("connected_slaves:").append(slaves.size()).append("\n");
            int idx = 0;
            for (SlaveInfo slave : slaves.values()) {
                sb.append("slave").append(idx++).append(":")
                  .append(slave.address).append(",")
                  .append(slave.offset).append(",online\n");
            }
        } else {
            sb.append("master_host:").append(masterHost).append("\n");
            sb.append("master_port:").append(masterPort).append("\n");
            sb.append("master_link_status:").append(connected ? "up" : "down").append("\n");
        }

        return sb.toString();
    }

    // Getters
    public Role getRole() { return role; }
    public String getReplId() { return replId; }
    public long getReplOffset() { return replOffset.get(); }
    public int getSlaveCount() { return slaves.size(); }
}