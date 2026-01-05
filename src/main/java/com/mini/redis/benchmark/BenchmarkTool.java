package com.mini.redis.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Redis Performance Benchmark Tool (Simplified)
 *
 * Interview points covered:
 * 1. Throughput measurement (ops/sec)
 * 2. Latency measurement (percentiles)
 * 3. Concurrent client simulation
 * 4. Different command benchmarks
 * 5. JMH-style warmup and measurement
 *
 * @author Mini Redis
 */
public class BenchmarkTool {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTool.class);

    // Benchmark configuration
    private int numClients = 50;
    private int numRequests = 100000;
    private int pipelineSize = 1;
    private int dataSize = 100; // bytes
    private int warmupIterations = 10000;

    // Metrics
    private final AtomicLong totalOps = new AtomicLong(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private final ConcurrentLinkedQueue<Long> latencies = new ConcurrentLinkedQueue<>();

    // Thread pool for clients
    private ExecutorService executor;

    /**
     * Benchmark result
     */
    public static class BenchmarkResult {
        public final String command;
        public final long totalOps;
        public final double opsPerSec;
        public final double avgLatencyMs;
        public final double p50LatencyMs;
        public final double p95LatencyMs;
        public final double p99LatencyMs;
        public final long durationMs;

        public BenchmarkResult(String command, long totalOps, double opsPerSec,
                              double avgLatencyMs, double p50LatencyMs,
                              double p95LatencyMs, double p99LatencyMs,
                              long durationMs) {
            this.command = command;
            this.totalOps = totalOps;
            this.opsPerSec = opsPerSec;
            this.avgLatencyMs = avgLatencyMs;
            this.p50LatencyMs = p50LatencyMs;
            this.p95LatencyMs = p95LatencyMs;
            this.p99LatencyMs = p99LatencyMs;
            this.durationMs = durationMs;
        }

        @Override
        public String toString() {
            return String.format(
                "Command: %s\n" +
                "Total Operations: %d\n" +
                "Throughput: %.2f ops/sec\n" +
                "Avg Latency: %.3f ms\n" +
                "P50 Latency: %.3f ms\n" +
                "P95 Latency: %.3f ms\n" +
                "P99 Latency: %.3f ms\n" +
                "Duration: %d ms",
                command, totalOps, opsPerSec, avgLatencyMs,
                p50LatencyMs, p95LatencyMs, p99LatencyMs, durationMs
            );
        }
    }

    /**
     * Run SET benchmark
     */
    public BenchmarkResult benchmarkSet() {
        logger.info("Starting SET benchmark: {} clients, {} requests",
                   numClients, numRequests);

        reset();
        executor = Executors.newFixedThreadPool(numClients);

        // Warmup
        performWarmup("SET");

        // Actual benchmark
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(numClients);

        int requestsPerClient = numRequests / numClients;
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;
            executor.submit(() -> {
                for (int j = 0; j < requestsPerClient; j++) {
                    String key = "key:" + clientId + ":" + j;
                    String value = generateValue(dataSize);

                    long opStart = System.nanoTime();
                    // Simulate SET operation
                    simulateSet(key, value);
                    long opLatency = System.nanoTime() - opStart;

                    recordOperation(opLatency);
                }
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long duration = System.currentTimeMillis() - startTime;
        executor.shutdown();

        return calculateResult("SET", duration);
    }

    /**
     * Run GET benchmark
     */
    public BenchmarkResult benchmarkGet() {
        logger.info("Starting GET benchmark: {} clients, {} requests",
                   numClients, numRequests);

        reset();
        executor = Executors.newFixedThreadPool(numClients);

        // Prepare data
        prepareData();

        // Warmup
        performWarmup("GET");

        // Actual benchmark
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(numClients);

        int requestsPerClient = numRequests / numClients;
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;
            executor.submit(() -> {
                for (int j = 0; j < requestsPerClient; j++) {
                    String key = "key:" + clientId + ":" + (j % 1000);

                    long opStart = System.nanoTime();
                    // Simulate GET operation
                    simulateGet(key);
                    long opLatency = System.nanoTime() - opStart;

                    recordOperation(opLatency);
                }
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long duration = System.currentTimeMillis() - startTime;
        executor.shutdown();

        return calculateResult("GET", duration);
    }

    /**
     * Run pipeline benchmark
     */
    public BenchmarkResult benchmarkPipeline() {
        logger.info("Starting PIPELINE benchmark: {} clients, {} requests, pipeline size: {}",
                   numClients, numRequests, pipelineSize);

        reset();
        executor = Executors.newFixedThreadPool(numClients);

        // Actual benchmark
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(numClients);

        int requestsPerClient = numRequests / numClients;
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;
            executor.submit(() -> {
                for (int j = 0; j < requestsPerClient; j += pipelineSize) {
                    long opStart = System.nanoTime();

                    // Simulate pipeline
                    for (int k = 0; k < pipelineSize && (j + k) < requestsPerClient; k++) {
                        String key = "key:" + clientId + ":" + (j + k);
                        String value = generateValue(dataSize);
                        simulateSet(key, value);
                    }

                    long opLatency = System.nanoTime() - opStart;
                    recordOperation(opLatency);
                }
                latch.countDown();
            });
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long duration = System.currentTimeMillis() - startTime;
        executor.shutdown();

        return calculateResult("PIPELINE", duration);
    }

    /**
     * Perform warmup
     */
    private void performWarmup(String command) {
        logger.info("Performing warmup for {}: {} iterations", command, warmupIterations);

        for (int i = 0; i < warmupIterations; i++) {
            if (command.equals("SET")) {
                simulateSet("warmup:" + i, generateValue(dataSize));
            } else if (command.equals("GET")) {
                simulateGet("warmup:" + (i % 100));
            }
        }
    }

    /**
     * Prepare test data
     */
    private void prepareData() {
        logger.info("Preparing test data...");
        for (int i = 0; i < numClients; i++) {
            for (int j = 0; j < 1000; j++) {
                String key = "key:" + i + ":" + j;
                simulateSet(key, generateValue(dataSize));
            }
        }
    }

    /**
     * Simulate SET operation
     */
    private void simulateSet(String key, String value) {
        // In real implementation, would execute actual SET
        try {
            Thread.sleep(0, 100); // Simulate minimal latency
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Simulate GET operation
     */
    private String simulateGet(String key) {
        // In real implementation, would execute actual GET
        try {
            Thread.sleep(0, 100); // Simulate minimal latency
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return "value";
    }

    /**
     * Generate test value
     */
    private String generateValue(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }

    /**
     * Record operation metrics
     */
    private void recordOperation(long latencyNanos) {
        totalOps.incrementAndGet();
        totalLatency.addAndGet(latencyNanos);
        latencies.offer(latencyNanos / 1_000_000); // Convert to milliseconds
    }

    /**
     * Calculate benchmark result
     */
    private BenchmarkResult calculateResult(String command, long durationMs) {
        long ops = totalOps.get();
        double opsPerSec = (ops * 1000.0) / durationMs;
        double avgLatencyMs = (totalLatency.get() / 1_000_000.0) / ops;

        // Calculate percentiles
        Long[] latencyArray = latencies.toArray(new Long[0]);
        java.util.Arrays.sort(latencyArray);

        double p50 = getPercentile(latencyArray, 50);
        double p95 = getPercentile(latencyArray, 95);
        double p99 = getPercentile(latencyArray, 99);

        return new BenchmarkResult(command, ops, opsPerSec, avgLatencyMs,
                                  p50, p95, p99, durationMs);
    }

    /**
     * Get percentile value
     */
    private double getPercentile(Long[] sorted, int percentile) {
        if (sorted.length == 0) return 0;
        int index = (sorted.length * percentile) / 100;
        if (index >= sorted.length) index = sorted.length - 1;
        return sorted[index];
    }

    /**
     * Reset metrics
     */
    private void reset() {
        totalOps.set(0);
        totalLatency.set(0);
        latencies.clear();
    }

    /**
     * Run all benchmarks
     */
    public void runAllBenchmarks() {
        System.out.println("=== Redis Performance Benchmarks ===\n");

        // SET benchmark
        BenchmarkResult setResult = benchmarkSet();
        System.out.println("SET Benchmark:");
        System.out.println(setResult);
        System.out.println();

        // GET benchmark
        BenchmarkResult getResult = benchmarkGet();
        System.out.println("GET Benchmark:");
        System.out.println(getResult);
        System.out.println();

        // Pipeline benchmark
        setPipelineSize(10);
        BenchmarkResult pipelineResult = benchmarkPipeline();
        System.out.println("PIPELINE Benchmark:");
        System.out.println(pipelineResult);
        System.out.println();

        System.out.println("=== Benchmarks Complete ===");
    }

    // Configuration setters
    public void setNumClients(int numClients) {
        this.numClients = numClients;
    }

    public void setNumRequests(int numRequests) {
        this.numRequests = numRequests;
    }

    public void setPipelineSize(int pipelineSize) {
        this.pipelineSize = pipelineSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    /**
     * Main method for standalone execution
     */
    public static void main(String[] args) {
        BenchmarkTool benchmark = new BenchmarkTool();
        benchmark.runAllBenchmarks();
    }
}