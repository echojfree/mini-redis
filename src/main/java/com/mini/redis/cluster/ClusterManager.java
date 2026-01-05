package com.mini.redis.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC16;

/**
 * Redis Cluster Manager (Simplified)
 *
 * Interview points covered:
 * 1. Hash slot distribution (16384 slots)
 * 2. CRC16 hashing for key distribution
 * 3. Cluster topology management
 * 4. Resharding support
 * 5. Cluster bus communication
 *
 * @author Mini Redis
 */
public class ClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    // Total hash slots in Redis Cluster
    private static final int HASH_SLOT_COUNT = 16384;

    // Singleton instance
    private static volatile ClusterManager instance;

    // Cluster nodes
    private final ConcurrentHashMap<String, ClusterNode> nodes = new ConcurrentHashMap<>();

    // Hash slot assignments
    private final ClusterNode[] slotAssignments = new ClusterNode[HASH_SLOT_COUNT];

    // Current node ID
    private String nodeId;
    private ClusterNode currentNode;

    // Cluster state
    private boolean clusterEnabled = false;

    /**
     * Cluster node information
     */
    public static class ClusterNode {
        public final String nodeId;
        public String host;
        public int port;
        public int clusterPort; // Cluster bus port
        public boolean isMaster;
        public String masterId; // For slaves
        public int slotStart = -1;
        public int slotEnd = -1;

        public ClusterNode(String nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
            this.clusterPort = port + 10000; // Default cluster bus port
            this.isMaster = true;
        }
    }

    private ClusterManager() {
        this.nodeId = generateNodeId();
    }

    /**
     * Get singleton instance
     */
    public static ClusterManager getInstance() {
        if (instance == null) {
            synchronized (ClusterManager.class) {
                if (instance == null) {
                    instance = new ClusterManager();
                }
            }
        }
        return instance;
    }

    /**
     * Initialize cluster mode
     */
    public void initCluster(String host, int port) {
        this.currentNode = new ClusterNode(nodeId, host, port);
        nodes.put(nodeId, currentNode);
        clusterEnabled = true;
        logger.info("Cluster initialized with node ID: {}", nodeId);
    }

    /**
     * Add cluster node
     */
    public void addNode(String nodeId, String host, int port, boolean isMaster) {
        ClusterNode node = new ClusterNode(nodeId, host, port);
        node.isMaster = isMaster;
        nodes.put(nodeId, node);
        logger.info("Added cluster node: {} at {}:{}", nodeId, host, port);
    }

    /**
     * Assign hash slots to node
     */
    public void assignSlots(String nodeId, int startSlot, int endSlot) {
        ClusterNode node = nodes.get(nodeId);
        if (node == null) {
            logger.error("Node not found: {}", nodeId);
            return;
        }

        // Validate slot range
        if (startSlot < 0 || endSlot >= HASH_SLOT_COUNT || startSlot > endSlot) {
            logger.error("Invalid slot range: {}-{}", startSlot, endSlot);
            return;
        }

        // Assign slots
        for (int slot = startSlot; slot <= endSlot; slot++) {
            slotAssignments[slot] = node;
        }

        node.slotStart = startSlot;
        node.slotEnd = endSlot;

        logger.info("Assigned slots {}-{} to node {}", startSlot, endSlot, nodeId);
    }

    /**
     * Calculate hash slot for a key
     */
    public int calculateHashSlot(String key) {
        // Extract hash tag if present
        int start = key.indexOf('{');
        if (start != -1) {
            int end = key.indexOf('}', start + 1);
            if (end != -1 && end != start + 1) {
                key = key.substring(start + 1, end);
            }
        }

        // Calculate CRC16 and get slot
        return crc16(key.getBytes()) & (HASH_SLOT_COUNT - 1);
    }

    /**
     * Get node responsible for a key
     */
    public ClusterNode getNodeForKey(String key) {
        if (!clusterEnabled) {
            return currentNode;
        }

        int slot = calculateHashSlot(key);
        return slotAssignments[slot];
    }

    /**
     * Check if current node owns the key
     */
    public boolean isLocalKey(String key) {
        if (!clusterEnabled) {
            return true;
        }

        ClusterNode responsible = getNodeForKey(key);
        return responsible != null && responsible.nodeId.equals(nodeId);
    }

    /**
     * Get redirect information for key
     */
    public String getRedirect(String key, boolean isWrite) {
        ClusterNode responsible = getNodeForKey(key);

        if (responsible == null) {
            return null; // No node owns this slot
        }

        if (responsible.nodeId.equals(nodeId)) {
            return null; // We own this key
        }

        // For writes, must redirect to master
        if (isWrite && !responsible.isMaster) {
            // Find master for this slave
            ClusterNode master = findMaster(responsible.masterId);
            if (master != null) {
                responsible = master;
            }
        }

        int slot = calculateHashSlot(key);
        return String.format("MOVED %d %s:%d", slot, responsible.host, responsible.port);
    }

    /**
     * Find master node by ID
     */
    private ClusterNode findMaster(String masterId) {
        if (masterId == null) {
            return null;
        }
        return nodes.get(masterId);
    }

    /**
     * Perform resharding (simplified)
     */
    public void reshard(String sourceNodeId, String targetNodeId, int slotCount) {
        logger.info("Resharding {} slots from {} to {}", slotCount, sourceNodeId, targetNodeId);

        ClusterNode source = nodes.get(sourceNodeId);
        ClusterNode target = nodes.get(targetNodeId);

        if (source == null || target == null) {
            logger.error("Invalid nodes for resharding");
            return;
        }

        // Simplified: just reassign slots
        int movedCount = 0;
        for (int slot = source.slotStart; slot <= source.slotEnd && movedCount < slotCount; slot++) {
            if (slotAssignments[slot] == source) {
                slotAssignments[slot] = target;
                movedCount++;
            }
        }

        logger.info("Resharding completed: moved {} slots", movedCount);
    }

    /**
     * CRC16 implementation for hash slot calculation
     */
    private int crc16(byte[] bytes) {
        int crc = 0;
        for (byte b : bytes) {
            crc = ((crc << 8) ^ CRC16_TABLE[((crc >> 8) ^ (b & 0xFF)) & 0xFF]) & 0xFFFF;
        }
        return crc & 0xFFFF;
    }

    // CRC16 lookup table (partial, for demo)
    private static final int[] CRC16_TABLE = new int[256];
    static {
        // Initialize CRC16 table (simplified)
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int j = 0; j < 8; j++) {
                if ((crc & 0x8000) != 0) {
                    crc = (crc << 1) ^ 0x1021;
                } else {
                    crc <<= 1;
                }
            }
            CRC16_TABLE[i] = crc & 0xFFFF;
        }
    }

    /**
     * Generate node ID
     */
    private String generateNodeId() {
        return Long.toHexString(System.currentTimeMillis()) +
               Long.toHexString(System.nanoTime()).substring(0, 8);
    }

    /**
     * Get cluster info
     */
    public String getClusterInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("cluster_enabled:").append(clusterEnabled ? 1 : 0).append("\n");
        sb.append("cluster_node_id:").append(nodeId).append("\n");
        sb.append("cluster_nodes_count:").append(nodes.size()).append("\n");

        if (currentNode != null) {
            sb.append("cluster_slots_assigned:")
              .append(currentNode.slotEnd >= 0 ? currentNode.slotEnd - currentNode.slotStart + 1 : 0)
              .append("\n");
        }

        sb.append("cluster_size:").append(HASH_SLOT_COUNT).append("\n");

        return sb.toString();
    }

    /**
     * Get nodes info
     */
    public String getNodesInfo() {
        StringBuilder sb = new StringBuilder();

        for (ClusterNode node : nodes.values()) {
            sb.append(node.nodeId).append(" ")
              .append(node.host).append(":").append(node.port).append(" ")
              .append(node.isMaster ? "master" : "slave").append(" ");

            if (node.slotStart >= 0) {
                sb.append(node.slotStart).append("-").append(node.slotEnd);
            }

            sb.append("\n");
        }

        return sb.toString();
    }

    // Getters
    public boolean isClusterEnabled() { return clusterEnabled; }
    public String getNodeId() { return nodeId; }
    public int getNodeCount() { return nodes.size(); }
}