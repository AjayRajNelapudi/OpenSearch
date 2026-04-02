/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for NativeExecutorTracker as used by DataFusionPlugin.
 */
class DataFusionPluginSettingsTests {

    @Test
    void testTrackerStartsAtZero() {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test");
        assertEquals(0, tracker.getNativeInFlight());
        assertEquals(0, tracker.getNativeAcquired());
    }

    @Test
    void testAcquireIncrementsCounters() {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test");
        tracker.acquire();
        assertEquals(1, tracker.getNativeInFlight());
        assertEquals(1, tracker.getNativeAcquired());
    }

    @Test
    void testAcquireReleaseRoundTrip() {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test");
        tracker.acquire();
        tracker.acquire();
        assertEquals(2, tracker.getNativeInFlight());
        tracker.release();
        tracker.release();
        assertEquals(0, tracker.getNativeInFlight());
        assertEquals(2, tracker.getNativeAcquired());
    }
}
