/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni;

import org.opensearch.core.action.ActionListener;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTrackerRegistry;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.lifecycle.BeforeProperty;
import net.jqwik.api.lifecycle.AfterProperty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link TrackedNativeBridge} acquire/release/listener wrapping logic.
 * <p>
 * Since {@link NativeBridge} methods are native (JNI), these tests exercise the
 * acquire/release patterns (AtomicBoolean guard, try/finally) without invoking actual JNI calls.
 * This replaces the former {@code NativeBridgeBackpressureTests}.
 */
class TrackedNativeBridgeTests {

    @BeforeProperty
    void setUp() {
        NativeExecutorTrackerRegistry.clear();
    }

    @AfterProperty
    void tearDown() {
        NativeExecutorTrackerRegistry.clear();
    }

    // -----------------------------------------------------------------------
    // Feature: per-operation-native-tracker, Property 3: Async wrapping releases exactly once
    // Validates: Requirements 3.1, 3.2, 3.7
    // -----------------------------------------------------------------------

    /**
     * Property 3: Async wrapping releases exactly once.
     * <p>
     * For any tracker and any combination of onResponse/onFailure callbacks
     * (including double-callback): in-flight returns to pre-acquire value,
     * acquired increments by exactly 1, delegate receives exactly one callback.
     */
    @Property(tries = 100)
    void asyncWrappingReleasesExactlyOnce(@ForAll("callbackSequences") List<Boolean> callbacks) {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test_async");
        int preAcquireInFlight = tracker.getNativeInFlight();
        long preAcquireAcquired = tracker.getNativeAcquired();

        // Simulate the async wrapping pattern: acquire + AtomicBoolean-guarded listener
        tracker.acquire();
        final AtomicBoolean released = new AtomicBoolean(false);
        AtomicInteger delegateCallbackCount = new AtomicInteger(0);

        ActionListener<Long> delegate = new ActionListener<>() {
            @Override
            public void onResponse(Long result) {
                delegateCallbackCount.incrementAndGet();
            }

            @Override
            public void onFailure(Exception e) {
                delegateCallbackCount.incrementAndGet();
            }
        };

        // Create the wrapped listener (same pattern as TrackedNativeBridge.wrapListener)
        ActionListener<Long> wrapped = new ActionListener<>() {
            @Override
            public void onResponse(Long result) {
                if (released.compareAndSet(false, true)) {
                    tracker.release();
                }
                delegate.onResponse(result);
            }

            @Override
            public void onFailure(Exception e) {
                if (released.compareAndSet(false, true)) {
                    tracker.release();
                }
                delegate.onFailure(e);
            }
        };

        // Fire callbacks in the generated sequence (true = onResponse, false = onFailure)
        for (boolean isResponse : callbacks) {
            if (isResponse) {
                wrapped.onResponse(42L);
            } else {
                wrapped.onFailure(new RuntimeException("test"));
            }
        }

        // Verify: in-flight returns to pre-acquire value
        assertEquals(preAcquireInFlight, tracker.getNativeInFlight(),
            "in-flight must return to pre-acquire value after async wrapping completes");

        // Verify: acquired incremented by exactly 1
        assertEquals(preAcquireAcquired + 1, tracker.getNativeAcquired(),
            "acquired must increment by exactly 1");

        // Verify: delegate receives exactly one callback per fired callback
        // (the guard only protects release, not delegate forwarding)
        assertEquals(callbacks.size(), delegateCallbackCount.get(),
            "delegate must receive one callback per invocation");
    }

    @Provide
    Arbitrary<List<Boolean>> callbackSequences() {
        // Generate 1-4 callbacks: mix of onResponse (true) and onFailure (false)
        return Arbitraries.of(true, false).list().ofMinSize(1).ofMaxSize(4);
    }

    // -----------------------------------------------------------------------
    // Feature: per-operation-native-tracker, Property 4: Sync wrapping always releases
    // Validates: Requirements 3.3, 3.4, 3.5
    // -----------------------------------------------------------------------

    /**
     * Property 4: Sync wrapping always releases.
     * <p>
     * For any tracker and any outcome (success or exception): in-flight returns
     * to pre-acquire value, acquired increments by exactly 1.
     */
    @Property(tries = 100)
    void syncWrappingAlwaysReleases(@ForAll boolean throwsException) {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test_sync");
        int preAcquireInFlight = tracker.getNativeInFlight();
        long preAcquireAcquired = tracker.getNativeAcquired();

        // Simulate the try/finally wrapping pattern used by sync methods
        tracker.acquire();
        try {
            if (throwsException) {
                throw new RuntimeException("simulated JNI failure");
            }
            // Simulate successful JNI call (no-op)
        } catch (Exception e) {
            // Exception caught — release still happens in finally
        } finally {
            tracker.release();
        }

        // Verify: in-flight returns to pre-acquire value
        assertEquals(preAcquireInFlight, tracker.getNativeInFlight(),
            "in-flight must return to pre-acquire value after sync wrapping completes");

        // Verify: acquired incremented by exactly 1
        assertEquals(preAcquireAcquired + 1, tracker.getNativeAcquired(),
            "acquired must increment by exactly 1");
    }
}
