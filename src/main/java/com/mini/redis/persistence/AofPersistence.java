package com.mini.redis.persistence;

import com.mini.redis.protocol.RespMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AOF (Append-Only File) Persistence Manager
 *
 * Implements Redis AOF persistence:
 * - Command logging for durability
 * - Incremental persistence
 * - AOF rewriting for compaction
 * - Configurable fsync policies
 * - Background rewriting
 *
 * Interview points:
 * 1. Write-ahead logging (WAL)
 * 2. fsync policies: always, everysec, no
 * 3. AOF rewriting algorithm
 * 4. Buffer management
 * 5. Trade-offs vs RDB
 *
 * @author Mini Redis
 */
public class AofPersistence {

    private static final Logger logger = LoggerFactory.getLogger(AofPersistence.class);

    // AOF fsync policies
    public enum FsyncPolicy {
        ALWAYS,     // fsync after every write (safest, slowest)
        EVERYSEC,   // fsync every second (balanced)
        NO          // Let OS decide when to fsync (fastest, least safe)
    }

    // Singleton instance
    private static volatile AofPersistence instance;

    // Configuration
    private String aofFilePath = "appendonly.aof";
    private String tempAofFilePath = "temp-appendonly.aof";
    private FsyncPolicy fsyncPolicy = FsyncPolicy.EVERYSEC;
    private boolean aofEnabled = false;
    private long aofRewriteMinSize = 64 * 1024 * 1024; // 64MB
    private int aofRewritePercentage = 100; // Double size triggers rewrite

    // AOF writer
    private BufferedWriter aofWriter;
    private FileOutputStream aofFileStream;
    private final Object writerLock = new Object();

    // Background thread for async writes
    private Thread aofThread;
    private final BlockingQueue<String> aofBuffer = new ArrayBlockingQueue<>(10000);
    private final AtomicBoolean aofThreadRunning = new AtomicBoolean(false);

    // Statistics
    private final AtomicLong aofCurrentSize = new AtomicLong(0);
    private final AtomicLong aofBaseSize = new AtomicLong(0);
    private volatile long lastFsyncTime = System.currentTimeMillis();

    // Background rewrite
    private Thread rewriteThread;
    private volatile boolean rewriteInProgress = false;

    private AofPersistence() {
    }

    /**
     * Get singleton instance
     */
    public static AofPersistence getInstance() {
        if (instance == null) {
            synchronized (AofPersistence.class) {
                if (instance == null) {
                    instance = new AofPersistence();
                }
            }
        }
        return instance;
    }

    /**
     * Start AOF persistence
     */
    public synchronized void start() throws IOException {
        if (aofEnabled) {
            logger.warn("AOF already enabled");
            return;
        }

        // Open AOF file for appending
        openAofFile(aofFilePath, true);

        // Start background thread
        startAofThread();

        aofEnabled = true;
        logger.info("AOF enabled with policy: {}", fsyncPolicy);
    }

    /**
     * Stop AOF persistence
     */
    public synchronized void stop() {
        if (!aofEnabled) {
            return;
        }

        aofEnabled = false;
        stopAofThread();
        closeAofFile();

        logger.info("AOF disabled");
    }

    /**
     * Append command to AOF
     */
    public void appendCommand(RespMessage command) {
        if (!aofEnabled) {
            return;
        }

        try {
            String cmdString = serializeCommand(command);

            if (fsyncPolicy == FsyncPolicy.ALWAYS) {
                // Synchronous write for ALWAYS policy
                synchronized (writerLock) {
                    writeToAof(cmdString);
                    fsyncAof();
                }
            } else {
                // Asynchronous write for other policies
                if (!aofBuffer.offer(cmdString, 100, TimeUnit.MILLISECONDS)) {
                    logger.warn("AOF buffer full, command dropped");
                }
            }

        } catch (Exception e) {
            logger.error("Failed to append command to AOF", e);
        }
    }

    /**
     * Load commands from AOF file
     */
    public List<RespMessage> loadAof() throws IOException {
        File aofFile = new File(aofFilePath);
        if (!aofFile.exists()) {
            logger.info("AOF file not found: {}", aofFilePath);
            return new ArrayList<>();
        }

        logger.info("Loading AOF file: {}", aofFilePath);
        List<RespMessage> commands = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(aofFile), StandardCharsets.UTF_8))) {

            String line;
            List<String> commandLines = new ArrayList<>();

            while ((line = reader.readLine()) != null) {
                if (line.startsWith("*")) {
                    // Start of new command
                    if (!commandLines.isEmpty()) {
                        RespMessage cmd = parseCommand(commandLines);
                        if (cmd != null) {
                            commands.add(cmd);
                        }
                    }
                    commandLines.clear();
                }
                commandLines.add(line);
            }

            // Parse last command
            if (!commandLines.isEmpty()) {
                RespMessage cmd = parseCommand(commandLines);
                if (cmd != null) {
                    commands.add(cmd);
                }
            }

        }

        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("AOF load completed: {} commands in {} ms", commands.size(), elapsed);
        return commands;
    }

    /**
     * Trigger AOF rewrite
     */
    public boolean rewriteAof() {
        if (rewriteInProgress) {
            logger.warn("AOF rewrite already in progress");
            return false;
        }

        rewriteThread = new Thread(() -> {
            rewriteInProgress = true;
            try {
                performRewrite();
            } catch (Exception e) {
                logger.error("AOF rewrite failed", e);
            } finally {
                rewriteInProgress = false;
            }
        }, "AOF-Rewrite");

        rewriteThread.start();
        logger.info("AOF rewrite started");
        return true;
    }

    /**
     * Perform AOF rewrite
     */
    private void performRewrite() throws IOException {
        logger.info("Starting AOF rewrite...");
        long startTime = System.currentTimeMillis();

        // Create temp file
        try (BufferedWriter tempWriter = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(tempAofFilePath), StandardCharsets.UTF_8))) {

            // Generate snapshot commands from current database state
            List<RespMessage> snapshotCommands = generateSnapshotCommands();

            // Write snapshot to temp file
            for (RespMessage cmd : snapshotCommands) {
                tempWriter.write(serializeCommand(cmd));
            }

            tempWriter.flush();
        }

        // Atomic rename
        synchronized (writerLock) {
            closeAofFile();

            File tempFile = new File(tempAofFilePath);
            File targetFile = new File(aofFilePath);

            if (targetFile.exists()) {
                targetFile.delete();
            }

            if (!tempFile.renameTo(targetFile)) {
                throw new IOException("Failed to rename temp AOF file");
            }

            // Reopen AOF file
            openAofFile(aofFilePath, true);
        }

        // Update statistics
        File newAofFile = new File(aofFilePath);
        aofBaseSize.set(newAofFile.length());
        aofCurrentSize.set(aofBaseSize.get());

        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("AOF rewrite completed in {} ms, new size: {} bytes",
                    elapsed, aofBaseSize.get());
    }

    /**
     * Generate snapshot commands for current database state
     */
    private List<RespMessage> generateSnapshotCommands() {
        List<RespMessage> commands = new ArrayList<>();

        // Note: In a real implementation, this would iterate through
        // all databases and generate appropriate SET/SADD/ZADD commands
        // to recreate the current state

        // For now, return empty list as we need DatabaseManager integration
        logger.info("Generated {} snapshot commands", commands.size());
        return commands;
    }

    /**
     * Serialize command to RESP protocol string
     */
    private String serializeCommand(RespMessage command) {
        if (command instanceof RespMessage.Array) {
            StringBuilder sb = new StringBuilder();
            RespMessage.Array array = (RespMessage.Array) command;
            List<RespMessage> elements = array.getElements();

            sb.append("*").append(elements.size()).append("\r\n");

            for (RespMessage element : elements) {
                if (element instanceof RespMessage.BulkString) {
                    RespMessage.BulkString bulk = (RespMessage.BulkString) element;
                    String value = bulk.getStringValue();
                    sb.append("$").append(value.length()).append("\r\n");
                    sb.append(value).append("\r\n");
                }
            }

            return sb.toString();
        }
        return "";
    }

    /**
     * Parse command from RESP lines
     */
    private RespMessage parseCommand(List<String> lines) {
        if (lines.isEmpty() || !lines.get(0).startsWith("*")) {
            return null;
        }

        try {
            int arraySize = Integer.parseInt(lines.get(0).substring(1));
            List<RespMessage> elements = new ArrayList<>();
            int lineIndex = 1;

            for (int i = 0; i < arraySize; i++) {
                if (lineIndex >= lines.size()) break;

                String lengthLine = lines.get(lineIndex++);
                if (!lengthLine.startsWith("$")) continue;

                int length = Integer.parseInt(lengthLine.substring(1));
                if (lineIndex >= lines.size()) break;

                String value = lines.get(lineIndex++);
                elements.add(new RespMessage.BulkString(value));
            }

            return new RespMessage.Array(elements);

        } catch (Exception e) {
            logger.error("Failed to parse command from lines", e);
            return null;
        }
    }

    /**
     * Start background AOF thread
     */
    private void startAofThread() {
        aofThreadRunning.set(true);
        aofThread = new Thread(() -> {
            while (aofThreadRunning.get()) {
                try {
                    String command = aofBuffer.poll(100, TimeUnit.MILLISECONDS);
                    if (command != null) {
                        synchronized (writerLock) {
                            writeToAof(command);
                        }
                    }

                    // Handle fsync based on policy
                    if (fsyncPolicy == FsyncPolicy.EVERYSEC) {
                        long now = System.currentTimeMillis();
                        if (now - lastFsyncTime > 1000) {
                            synchronized (writerLock) {
                                fsyncAof();
                            }
                            lastFsyncTime = now;
                        }
                    }

                    // Check if rewrite is needed
                    checkRewriteCondition();

                } catch (Exception e) {
                    logger.error("AOF thread error", e);
                }
            }
        }, "AOF-Writer");
        aofThread.start();
    }

    /**
     * Stop background AOF thread
     */
    private void stopAofThread() {
        aofThreadRunning.set(false);
        if (aofThread != null) {
            try {
                aofThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Write to AOF file
     */
    private void writeToAof(String command) throws IOException {
        if (aofWriter != null) {
            aofWriter.write(command);
            aofWriter.flush();
            aofCurrentSize.addAndGet(command.getBytes(StandardCharsets.UTF_8).length);
        }
    }

    /**
     * Force fsync on AOF file
     */
    private void fsyncAof() throws IOException {
        if (aofFileStream != null) {
            aofFileStream.getFD().sync();
        }
    }

    /**
     * Open AOF file
     */
    private void openAofFile(String path, boolean append) throws IOException {
        aofFileStream = new FileOutputStream(path, append);
        aofWriter = new BufferedWriter(
            new OutputStreamWriter(aofFileStream, StandardCharsets.UTF_8));

        if (append) {
            File file = new File(path);
            aofCurrentSize.set(file.length());
            aofBaseSize.set(file.length());
        }
    }

    /**
     * Close AOF file
     */
    private void closeAofFile() {
        try {
            if (aofWriter != null) {
                aofWriter.close();
                aofWriter = null;
            }
            if (aofFileStream != null) {
                aofFileStream.close();
                aofFileStream = null;
            }
        } catch (IOException e) {
            logger.error("Failed to close AOF file", e);
        }
    }

    /**
     * Check if rewrite is needed
     */
    private void checkRewriteCondition() {
        if (rewriteInProgress) {
            return;
        }

        long currentSize = aofCurrentSize.get();
        long baseSize = aofBaseSize.get();

        // Check size threshold
        if (currentSize < aofRewriteMinSize) {
            return;
        }

        // Check percentage growth
        if (baseSize > 0) {
            long growthPercentage = ((currentSize - baseSize) * 100) / baseSize;
            if (growthPercentage >= aofRewritePercentage) {
                logger.info("AOF rewrite triggered: size={}, growth={}%",
                           currentSize, growthPercentage);
                rewriteAof();
            }
        }
    }

    /**
     * Configuration methods
     */
    public void setFsyncPolicy(FsyncPolicy policy) {
        this.fsyncPolicy = policy;
        logger.info("AOF fsync policy set to: {}", policy);
    }

    public FsyncPolicy getFsyncPolicy() {
        return fsyncPolicy;
    }

    public void setAofFilePath(String path) {
        this.aofFilePath = path;
        this.tempAofFilePath = path + ".tmp";
    }

    public String getAofFilePath() {
        return aofFilePath;
    }

    public void setRewriteMinSize(long size) {
        this.aofRewriteMinSize = size;
    }

    public void setRewritePercentage(int percentage) {
        this.aofRewritePercentage = percentage;
    }

    public boolean isRewriteInProgress() {
        return rewriteInProgress;
    }

    public long getAofCurrentSize() {
        return aofCurrentSize.get();
    }

    public boolean isEnabled() {
        return aofEnabled;
    }
}