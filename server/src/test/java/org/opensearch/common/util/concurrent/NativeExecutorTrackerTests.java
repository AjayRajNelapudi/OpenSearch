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
import net.jqwik.api.Provide;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.constraints.Size;

import org.junit.jupiter.api.Test;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based and unit tests for {@link NativeExecutorTracker}.
 */
public class NativeExecutorTrackerTests {

    // Feature: per-operation-native-tracker, Property 1: Name round-trip
    // **Validates: Requirements 1.1, 1.2**

    @Property(tries = 100)
    void nameRoundTrip(@ForAll String name) {
        NativeExecutorTracker tracker = new NativeExecutorTracker(name);
        assertEquals(name, tracker.getName(), "getName() must return the name provided at construction");
    }

    // Feature: per-operation-native-tracker, Property 2: Tracker counter invariant
    // **Validates: Requirements 1.3, 1.5**

    @Property(tries = 100)
    void trackerCounterInvariant(@ForAll("acquireReleaseSequence") List<Boolean> ops) {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test");

        int acquires = 0;
        int expectedInFlight = 0;

        for (boolean isAcquire : ops) {
            if (isAcquire) {
                tracker.acquire();
                acquires++;
                expectedInFlight++;
            } else {
                tracker.release();
                expectedInFlight = Math.max(0, expectedInFlight - 1);
            }

            // In-flight must never be negative
            assertTrue(tracker.getNativeInFlight() >= 0, "getNativeInFlight() must never be negative");
        }

        assertEquals(expectedInFlight, tracker.getNativeInFlight(),
            "getNativeInFlight() must match expected in-flight after clamped release sequence");
        assertEquals(acquires, tracker.getNativeAcquired(),
            "getNativeAcquired() must equal total acquire count");
    }

    @Provide
    Arbitrary<List<Boolean>> acquireReleaseSequence() {
        return Arbitraries.of(true, false).list().ofMinSize(1).ofMaxSize(200);
    }

    // ---- Unit Tests ----

    @Test
    void nullNameThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> new NativeExecutorTracker(null));
    }

    @Test
    void releaseOnFreshTrackerStaysAtZero() {
        NativeExecutorTracker tracker = new NativeExecutorTracker("fresh");
        tracker.release();
        assertEquals(0, tracker.getNativeInFlight(), "release on fresh tracker must stay at 0");
        assertEquals(0, tracker.getNativeAcquired(), "acquired must remain 0 when only release is called");
    }

    @Test
    void multipleReleasesBelowZeroClampAtZero() {
        NativeExecutorTracker tracker = new NativeExecutorTracker("clamp");
        tracker.acquire();
        tracker.release();
        tracker.release();
        tracker.release();
        assertEquals(0, tracker.getNativeInFlight(), "in-flight must clamp at 0 after excess releases");
        assertEquals(1, tracker.getNativeAcquired(), "acquired must be 1 after single acquire");
    }
}
