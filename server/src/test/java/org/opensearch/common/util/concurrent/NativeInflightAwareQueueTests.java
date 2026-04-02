/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTrackerRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based and unit tests for {@link NativeInflightAwareQueue}.
 */
public class NativeInflightAwareQueueTests {

    @BeforeEach
    @BeforeProperty
    void setUp() {
        NativeExecutorTrackerRegistry.clear();
    }

    @AfterEach
    @AfterProperty
    void tearDown() {
        NativeExecutorTrackerRegistry.clear();
    }

    // Feature: tracker-registry-migration, Property 5: Queue rejection with registry-sourced trackers
    // **Validates: Requirements 6.1, 6.3, 6.4, 11.2**

    @Property(tries = 100)
    void queueRejectionWithMultiTrackerInflation(
        @ForAll @IntRange(min = 0, max = 50) int queueSize,
        @ForAll @IntRange(min = 1, max = 100) int capacity,
        @ForAll @Size(min = 1, max = 5) List<@IntRange(min = 0, max = 20) Integer> inFlightValues
    ) {
        // Register trackers in the registry, each with a different in-flight value
        List<NativeExecutorTracker> trackers = new ArrayList<>();
        int totalInFlight = 0;
        for (int i = 0; i < inFlightValues.size(); i++) {
            NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("op_" + i);
            int inFlight = inFlightValues.get(i);
            for (int j = 0; j < inFlight; j++) {
                tracker.acquire();
            }
            trackers.add(tracker);
            totalInFlight += inFlight;
        }

        NativeInflightAwareQueue<Runnable> queue = new NativeInflightAwareQueue<>(
            new LinkedTransferQueue<>(), capacity
        );

        // Pre-fill the queue
        int actualSize = 0;
        for (int i = 0; i < queueSize; i++) {
            try {
                queue.forcePut(() -> {});
                actualSize++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        int apparentSize = actualSize + totalInFlight;
        boolean result = queue.offer(() -> {});

        if (apparentSize >= capacity) {
            assertFalse(result, "offer() must return false when apparentSize (" + apparentSize + ") >= capacity (" + capacity + ")");
        } else {
            assertTrue(result, "offer() must return true when apparentSize (" + apparentSize + ") < capacity (" + capacity + ")");
        }

        // Clean up tracker state
        for (int i = 0; i < inFlightValues.size(); i++) {
            for (int j = 0; j < inFlightValues.get(i); j++) {
                trackers.get(i).release();
            }
        }
    }

    // Feature: tracker-registry-migration, Property 5 (empty registry fallback)
    // **Validates: Requirements 6.4**

    @Property(tries = 100)
    void emptyRegistryFallsBackToStandardBehavior(
        @ForAll @IntRange(min = 1, max = 50) int capacity,
        @ForAll @IntRange(min = 0, max = 60) int prefillCount
    ) {
        // Queue with empty registry (no trackers registered)
        NativeInflightAwareQueue<Runnable> nativeQueue = new NativeInflightAwareQueue<>(
            new LinkedTransferQueue<>(), capacity
        );
        ResizableBlockingQueue<Runnable> standardQueue = new ResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), capacity
        );

        for (int i = 0; i < prefillCount; i++) {
            try {
                nativeQueue.forcePut(() -> {});
                standardQueue.forcePut(() -> {});
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        assertEquals(standardQueue.size(), nativeQueue.size());

        Runnable task = () -> {};
        boolean nativeResult = nativeQueue.offer(task);
        boolean standardResult = standardQueue.offer(task);

        assertEquals(standardResult, nativeResult,
            "With empty registry, NativeInflightAwareQueue.offer() must match ResizableBlockingQueue.offer()");
    }

    // Feature: tracker-registry-migration, Queue capacity invariant (registry-based trackers)

    @Property(tries = 100)
    void queueCapacityInvariant(
        @ForAll @IntRange(min = 1, max = 100) int capacity,
        @ForAll @Size(min = 1, max = 200) List<@IntRange(min = 0, max = 4) Integer> operations
    ) {
        NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("test_op");
        NativeInflightAwareQueue<Runnable> queue = new NativeInflightAwareQueue<>(
            new LinkedTransferQueue<>(), capacity
        );

        assertEquals(capacity, queue.capacity());

        for (int op : operations) {
            switch (op) {
                case 0: queue.offer(() -> {}); break;
                case 1: queue.poll(); break;
                case 2: tracker.acquire(); break;
                case 3: tracker.release(); break;
            }
            assertEquals(capacity, queue.capacity(), "capacity() must remain unchanged after any operation");
        }
    }

    // ---- Unit Tests ----

    @Test
    void forcePutBypassesNativeAwareCapacityCheck() throws InterruptedException {
        int capacity = 5;
        NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("test_op");
        NativeInflightAwareQueue<Runnable> queue = new NativeInflightAwareQueue<>(
            new LinkedTransferQueue<>(), capacity
        );

        for (int i = 0; i < capacity; i++) {
            queue.forcePut(() -> {});
        }
        for (int i = 0; i < 10; i++) {
            tracker.acquire();
        }

        assertTrue(queue.size() + tracker.getNativeInFlight() >= capacity);
        assertFalse(queue.offer(() -> {}), "offer() must fail when apparent size >= capacity");

        queue.forcePut(() -> {});
        assertEquals(capacity + 1, queue.size(), "forcePut() must succeed even when apparent size >= capacity");

        for (int i = 0; i < 10; i++) {
            tracker.release();
        }
    }

    @Test
    void standardResizableBlockingQueueBehaviorWithoutTracker() {
        int capacity = 10;
        ResizableBlockingQueue<Runnable> queue = new ResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), capacity
        );

        for (int i = 0; i < capacity; i++) {
            assertTrue(queue.offer(() -> {}));
        }
        assertFalse(queue.offer(() -> {}));
        assertEquals(capacity, queue.size());

        queue.poll();
        assertTrue(queue.offer(() -> {}));
    }
}
