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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.LinkedTransferQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based and unit tests for {@link CompositeResizableBlockingQueue}.
 */
public class CompositeResizableBlockingQueueTests {

    // Feature: tracker-registry-migration, Property 5 (empty registry fallback)
    // **Validates: Requirements 6.4**

    @Property(tries = 100)
    void emptyRegistryFallsBackToStandardBehavior(
        @ForAll @IntRange(min = 1, max = 50) int capacity,
        @ForAll @IntRange(min = 0, max = 60) int prefillCount
    ) {
        // Queue with empty checks list (no trackers registered)
        CompositeResizableBlockingQueue<Runnable> nativeQueue = new CompositeResizableBlockingQueue<>(
            new LinkedTransferQueue<>(), capacity, List.of()
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
            "With empty checks list, CompositeResizableBlockingQueue.offer() must match ResizableBlockingQueue.offer()");
    }

    // ---- Unit Tests ----

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
