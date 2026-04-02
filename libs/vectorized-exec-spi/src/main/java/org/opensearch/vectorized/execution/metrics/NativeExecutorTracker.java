/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe per-operation counter for queries executing on the native Rust/Tokio runtime.
 * Each instance tracks a single JNI dispatch operation identified by {@link #getName()}.
 * <p>
 * Maintains an in-flight gauge and a cumulative acquired counter. The
 * {@link #acquire()} method unconditionally increments both counters.
 * The {@link #release()} method clamps at zero to guard against counter
 * underflow from buggy double-release scenarios.
 * <p>
 * Rejection is handled at the queue level by
 * {@code NativeInflightAwareQueue},
 * which inflates the search pool queue's apparent size by the sum of all
 * trackers' {@link #getNativeInFlight()} values.
 *
 * @opensearch.internal
 */
public final class NativeExecutorTracker {

    private final String name;
    private final AtomicInteger nativeInFlight = new AtomicInteger(0);
    private final AtomicLong nativeAcquired = new AtomicLong(0);

    public NativeExecutorTracker(String name) {
        this.name = Objects.requireNonNull(name, "tracker name must not be null");
    }

    /** Operation name matching the Rust-side Tokio task monitor name. */
    public String getName() {
        return name;
    }

    /**
     * Record that an operation has been dispatched to the native runtime.
     * Increments both the in-flight gauge and the cumulative acquired counter.
     */
    public void acquire() {
        nativeInFlight.incrementAndGet();
        nativeAcquired.incrementAndGet();
    }

    /**
     * Release a permit after native execution completes (success or failure).
     * Uses {@code getAndUpdate} to clamp at zero, preventing underflow.
     */
    public void release() {
        nativeInFlight.getAndUpdate(v -> Math.max(0, v - 1));
    }

    /** Current number of operations in flight. */
    public int getNativeInFlight() {
        return nativeInFlight.get();
    }

    /** Cumulative operations dispatched to native runtime since node start. */
    public long getNativeAcquired() {
        return nativeAcquired.get();
    }
}
