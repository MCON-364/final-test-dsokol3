package edu.touro.las.mcon364.final_test;

import java.util.DoubleSummaryStatistics;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TelemetryProcessor – concurrent sensor-data pipeline
 * <p>
 * Scenario: a fleet of devices continuously emits telemetry readings.
 * Each reading is represented as a {@link TelemetryEvent} carrying a device id,
 * a numeric metric value, and a nanosecond timestamp. Readings arrive faster than
 * they can be processed synchronously, so a multi-worker, queue-based pipeline
 * is required.
 * <p>
 * Requirements:
 * - submit(event) enqueues an event so a worker thread can process it.
 * It must throw {@link IllegalArgumentException} if event is null.
 * Events submitted before start() is called must be silently discarded.
 * - start(workerCount) spins up {@code workerCount} worker threads that continuously
 * drain the queue and process events. It must throw {@link IllegalArgumentException}
 * if workerCount ≤ 0. Calling start() a second time must be a no-op(should make no difference).
 * - stop() signals all workers to finish, waits for them to terminate, then processes
 * any events still left in the queue before returning.
 * - getTotalProcessed() returns the running total of events fully processed.
 * - getStats() returns a {@link DoubleSummaryStatistics} snapshot of all processed
 * metric values. Each call must return a fresh, independent object.
 * <p>
 * Thread-safety requirements:
 * - submit() and the read methods (getTotalProcessed, getStats) may be called
 * concurrently from multiple threads without data loss or corruption.
 * - Use java.util.concurrent building blocks. Do not use raw synchronized blocks.
 */
public class TelemetryProcessor {

    // ── declare whatever fields you need ─────────────────────────────────────
    private final BlockingQueue<TelemetryEvent> queue = new LinkedBlockingQueue<TelemetryEvent>();
    private volatile boolean running = false;
    private ExecutorService executor;
    private final DoubleSummaryStatistics stats = new DoubleSummaryStatistics();

    // ── public API ────────────────────────────────────────────────────────────

    /**
     * Add an event to the processing queue.
     * <p>
     * Events submitted before {@link #start(int)} is called must be silently discarded.
     *
     * @param event the telemetry event to enqueue; must not be null
     * @throws IllegalArgumentException if event is null
     */
    public void submit(TelemetryEvent event) throws IllegalArgumentException {
        //TODO - implement this method
        if (event == null) {
            throw new IllegalArgumentException("event can't be null");
        }

        if (!running) {
            throw new IllegalStateException("Processor is not running");
        }
        queue.offer(event);
    }

    /**
     * Start processing events.
     *
     * @param workerCount number of worker threads to create; must be ≥ 1
     * @throws IllegalArgumentException if workerCount ≤ 0
     */
    public void start(int workerCount) {
        //TODO - implement this method

        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive");
        }

        if (running) {
            throw new IllegalStateException("Processor already started");
        }

        running = true;
        executor = Executors.newFixedThreadPool(workerCount);

        for (int i = 0; i < workerCount; i++) {
            executor.submit(() -> {
                try {
                    workerLoop();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    /**
     * helper method
     */
    private void workerLoop() throws InterruptedException {
        // TODO: implement
        while (running || !queue.isEmpty()) {
            try{
            TelemetryEvent event = queue.poll(100, TimeUnit.MILLISECONDS);

            if (event != null) {
                process(event);
            }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
    private synchronized void process(TelemetryEvent event) {
        stats.accept(event.metric());
    }


    /**
     * Stop processing events.
     *
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void stop() throws InterruptedException {
        //TODO - implement this method
        running = false;

        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    /**
     * Return the total number of events that have been fully processed.
     */
    public int getTotalProcessed() {
        //TODO - implement this method
        return (int) stats.getCount();
    }

    /**
     * Return a point-in-time snapshot of summary statistics for all processed
     * metric values (count, sum, min, max, average).
     * <p>
     * Each call must return a <em>new</em>, independent {@link DoubleSummaryStatistics}
     * object so that callers cannot corrupt the internal state.
     *
     */
    public DoubleSummaryStatistics getStats() {
        //TODO - implement this method
        DoubleSummaryStatistics copy = new DoubleSummaryStatistics();
        copy.combine(stats);
        return copy;
    }
}
