/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTrackerRegistry;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;

/**
 * Extends {@link ResizableBlockingQueue} to inflate the apparent queue size by the
 * sum of all per-operation native in-flight counts. The {@link #offer(Object)} method
 * computes {@code apparentSize = queue.size() + sum(tracker.getNativeInFlight())}
 * and rejects when {@code apparentSize >= capacity()}, causing the existing
 * {@code OpenSearchAbortPolicy} to fire.
 * <p>
 * When the registry contains no trackers (plugin not installed or not yet registered),
 * {@code offer()} falls back to the standard {@link SizeBlockingQueue#offer(Object)}
 * behavior — zero behavioral change.
 * <p>
 * {@link #forcePut(Object)} is inherited from {@link SizeBlockingQueue} and bypasses
 * the native-aware check, so force-execution tasks are never rejected by this queue.
 * {@link #adjustCapacity(int, int, int, int)} is inherited from {@link ResizableBlockingQueue}
 * and continues to work unchanged.
 *
 * @opensearch.internal
 */
final class NativeInflightAwareQueue<E> extends ResizableBlockingQueue<E> {

    NativeInflightAwareQueue(BlockingQueue<E> queue, int initialCapacity) {
        super(queue, initialCapacity);
    }

    @Override
    public boolean offer(E e) {
        final Collection<NativeExecutorTracker> t = NativeExecutorTrackerRegistry.getAll();
        if (t.isEmpty()) {
            // No trackers — fall back to standard ResizableBlockingQueue behavior
            return super.offer(e);
        }
        while (true) {
            final int current = size.get();
            int totalNativeInFlight = 0;
            for (NativeExecutorTracker tracker : t) {
                totalNativeInFlight += tracker.getNativeInFlight();
            }
            final int apparentSize = current + totalNativeInFlight;
            if (apparentSize >= capacity()) {
                return false;
            }
            if (size.compareAndSet(current, 1 + current)) {
                break;
            }
        }
        boolean offered = queue.offer(e);
        if (!offered) {
            size.decrementAndGet();
        }
        return offered;
    }
}
