/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.vectorized.execution.metrics.NativeExecutorTrackerRegistry;
import org.opensearch.vectorized.execution.metrics.NativeMetricsSnapshot;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based and unit tests for {@link CompositeResizableBlockingQueue}
 * with the pluggable {@link QueueRejectionCheck} chain (Phase 2 Tokio metrics).
 */
public class CompositeResizableBlockingQueueTokioMetricsTests {

    @BeforeEach
    @BeforeProperty
    void setUp() {
        NativeMetricsSnapshot.set(null);
        NativeExecutorTrackerRegistry.clear();
    }

    @AfterEach
    @AfterProperty
    void tearDown() {
        NativeMetricsSnapshot.set(null);
        NativeExecutorTrackerRegistry.clear();
    }

    // --- Test helpers ---

    /**
     * A QueueRejectionCheck that always rejects.
     */
    static class AlwaysRejectCheck implements QueueRejectionCheck {
        @Override
        public boolean shouldReject() {
            return true;
        }
    }

    /**
     * A QueueRejectionCheck that never rejects.
     */
    static class NeverRejectCheck implements QueueRejectionCheck {
        @Override
        public boolean shouldReject() {
            return false;
        }
    }

    /**
     * A QueueRejectionCheck that tracks whether shouldReject() was called.
     * Used to verify short-circuit behavior.
     */
    static class TrackingCheck implements QueueRejectionCheck {
        private final AtomicBoolean called = new AtomicBoolean(false);

        @Override
        public boolean shouldReject() {
            called.set(true);
            return false; // never rejects — just records the call
        }

        boolean wasCalled() {
            return called.get();
        }

        void reset() {
            called.set(false);
        }
    }

    /**
     * A QueueRejectionCheck that rejects based on a controllable flag.
     * Used for the counter accuracy property test.
     */
    static class ControllableCheck implements QueueRejectionCheck {
        private final AtomicBoolean shouldReject = new AtomicBoolean(false);

        @Override
        public boolean shouldReject() {
            return shouldReject.get();
        }

        void setReject(boolean reject) {
            shouldReject.set(reject);
        }
    }

    // --- Generators ---

    @Provide
    Arbitrary<Integer> capacity() {
        return Arbitraries.integers().between(5, 50);
    }

    @Provide
    Arbitrary<List<Boolean>> rejectionSequence() {
        return Arbitraries.of(true, false).list().ofMinSize(1).ofMaxSize(50);
    }

    // --- Property Tests ---

    // Feature: tokio-metrics-rejection, Property 4: Rejection check chain short-circuits Phase 1
    // **Validates: Requirements 3.1, 3.3, 6.2**
    @Property(tries = 100)
    void rejectionCheckChainShortCircuitsPhase1(@ForAll("capacity") int cap) {
        // Put a rejecting check first, then a tracking check second.
        // If the first rejects, the second should NOT be called.
        TrackingCheck trackingCheck = new TrackingCheck();
        CompositeResizableBlockingQueue<Runnable> queue = new CompositeResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), cap, List.of(new AlwaysRejectCheck(), trackingCheck)
        );

        boolean result = queue.offer(() -> {});

        assertFalse(result, "offer() must return false when first check rejects");
        assertFalse(trackingCheck.wasCalled(),
            "Second check must NOT be called when first check already rejected");
    }

    // Feature: tokio-metrics-rejection, Property 5: Rejection counter accuracy
    // **Validates: Requirements 4.1**
    @Property(tries = 100)
    void rejectionCounterAccuracy(@ForAll("rejectionSequence") List<Boolean> sequence) {
        ControllableCheck controllableCheck = new ControllableCheck();
        AtomicInteger rejectionCounter = new AtomicInteger(0);

        // Wrap the controllable check to count rejections
        QueueRejectionCheck countingCheck = () -> {
            boolean reject = controllableCheck.shouldReject();
            if (reject) {
                rejectionCounter.incrementAndGet();
            }
            return reject;
        };

        CompositeResizableBlockingQueue<Runnable> queue = new CompositeResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), 1000, List.of(countingCheck)
        );

        int expectedRejections = 0;
        for (boolean shouldReject : sequence) {
            controllableCheck.setReject(shouldReject);
            boolean result = queue.offer(() -> {});
            if (shouldReject) {
                expectedRejections++;
                assertFalse(result, "offer() must return false when check rejects");
            }
        }

        assertEquals(expectedRejections, rejectionCounter.get(),
            "Rejection counter must equal the number of pre-check-caused rejections");
    }

    // --- Unit Tests (Task 5.5) ---

    @Test
    void emptyCheckList_superOfferRunsNormally() {
        // Empty check list → super.offer() runs normally (backward compatible)
        CompositeResizableBlockingQueue<Runnable> queue = new CompositeResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), 10, List.of()
        );

        assertTrue(queue.offer(() -> {}), "offer() must succeed with empty check list and available capacity");
        assertEquals(1, queue.size());
    }

    @Test
    void singleCheckThatRejects_offerReturnsFalse() {
        // Single check that rejects → offer() returns false without consulting later checks
        TrackingCheck trackingCheck = new TrackingCheck();
        CompositeResizableBlockingQueue<Runnable> queue = new CompositeResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), 10, List.of(new AlwaysRejectCheck(), trackingCheck)
        );

        assertFalse(queue.offer(() -> {}), "offer() must return false when check rejects");
        assertFalse(trackingCheck.wasCalled(), "Later check must not be called when earlier check rejects");
        assertEquals(0, queue.size(), "No task should be enqueued when check rejects");
    }

    @Test
    void singleCheckThatPasses_superOfferRunsNormally() {
        // Single check that passes → super.offer() runs normally
        CompositeResizableBlockingQueue<Runnable> queue = new CompositeResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), 10, List.of(new NeverRejectCheck())
        );

        assertTrue(queue.offer(() -> {}), "offer() must succeed when check passes and capacity available");
        assertEquals(1, queue.size());
    }

    @Test
    void multipleChecks_firstRejectingCheckShortCircuitsRemaining() {
        // Multiple checks → first rejecting check short-circuits remaining checks
        TrackingCheck secondTracking = new TrackingCheck();
        TrackingCheck thirdTracking = new TrackingCheck();
        CompositeResizableBlockingQueue<Runnable> queue = new CompositeResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), 10,
            List.of(new NeverRejectCheck(), new AlwaysRejectCheck(), secondTracking, thirdTracking)
        );

        assertFalse(queue.offer(() -> {}), "offer() must return false when second check rejects");
        assertFalse(secondTracking.wasCalled(), "Third check must not be called after second check rejects");
        assertFalse(thirdTracking.wasCalled(), "Fourth check must not be called after second check rejects");
    }

    @Test
    void existingPhase1BehaviorUnchangedWhenAllChecksPass() {
        // Test existing Phase 1 behavior unchanged when all checks pass
        // With all checks passing, the queue should behave like a standard ResizableBlockingQueue
        int capacity = 5;
        CompositeResizableBlockingQueue<Runnable> queue = new CompositeResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), capacity, List.of(new NeverRejectCheck())
        );

        // Fill to capacity
        for (int i = 0; i < capacity; i++) {
            assertTrue(queue.offer(() -> {}), "offer() must succeed when under capacity");
        }

        // At capacity, offer should fail via super.offer() capacity check
        assertFalse(queue.offer(() -> {}), "offer() must fail when queue is at capacity");
        assertEquals(capacity, queue.size());

        // After removing one, offer should succeed again
        queue.poll();
        assertTrue(queue.offer(() -> {}), "offer() must succeed after freeing capacity");
    }
}
