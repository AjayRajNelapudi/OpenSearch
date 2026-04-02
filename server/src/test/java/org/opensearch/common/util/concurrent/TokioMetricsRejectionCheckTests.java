/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;
import org.opensearch.vectorized.execution.metrics.NativeMetricsSnapshot;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterProperty;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based and unit tests for {@link TokioMetricsRejectionCheck}.
 */
public class TokioMetricsRejectionCheckTests {

    private final TokioMetricsRejectionCheck check = new TokioMetricsRejectionCheck();

    @AfterEach
    void resetSnapshotAfterUnit() {
        NativeMetricsSnapshot.set(null);
    }

    @AfterProperty
    void resetSnapshotAfterProperty() {
        NativeMetricsSnapshot.set(null);
    }


    // --- Helpers ---

    private static DataFusionPluginStats.TaskMonitorValues dummyTaskMonitor() {
        return new DataFusionPluginStats.TaskMonitorValues(0, 0, 0);
    }

    private static DataFusionPluginStats buildStats(DataFusionPluginStats.RuntimeValues cpuRuntime) {
        return new DataFusionPluginStats(
            new DataFusionPluginStats.RuntimeValues(2, 0, 0, 0, 0, 0), // io runtime
            cpuRuntime,
            dummyTaskMonitor(),
            dummyTaskMonitor(),
            dummyTaskMonitor(),
            dummyTaskMonitor(),
            dummyTaskMonitor()
        );
    }

    // --- Generators ---

    @Provide
    Arbitrary<Long> workersCount() {
        return Arbitraries.longs().between(1, 100);
    }

    @Provide
    Arbitrary<Long> globalQueueDepth() {
        return Arbitraries.longs().between(0, 2000);
    }

    // --- Property Tests ---

    // Feature: tokio-metrics-rejection, Property 3: TokioMetricsRejectionCheck rejection behavior
    // **Validates: Requirements 2.2, 2.6, 2.7**
    @Property(tries = 100)
    void rejectionBehaviorMatchesThreshold(@ForAll("workersCount") long workers, @ForAll("globalQueueDepth") long depth) {
        // Case 1: null snapshot → should not reject
        NativeMetricsSnapshot.set(null);
        assertFalse(check.shouldReject(), "Null snapshot must not reject");

        // Case 2: non-null snapshot with null CPU runtime → should not reject
        DataFusionPluginStats statsNullCpu = buildStats(null);
        NativeMetricsSnapshot.set(new NativeMetricsSnapshot(statsNullCpu, System.currentTimeMillis()));
        assertFalse(check.shouldReject(), "Null CPU runtime must not reject");

        // Case 3: non-null snapshot with valid CPU runtime → rejection iff depth >= workers * 10
        DataFusionPluginStats.RuntimeValues cpuRuntime = new DataFusionPluginStats.RuntimeValues(workers, 0, 0, 0, depth, 0);
        DataFusionPluginStats stats = buildStats(cpuRuntime);
        NativeMetricsSnapshot.set(new NativeMetricsSnapshot(stats, System.currentTimeMillis()));

        boolean expected = depth >= workers * 10;
        assertEquals(expected, check.shouldReject(),
            String.format("depth=%d, workers=%d, threshold=%d → expected shouldReject=%b", depth, workers, workers * 10, expected));

        // Reset for next iteration
        NativeMetricsSnapshot.set(null);
    }

    // --- Unit Tests ---

    @Test
    void nullSnapshot_shouldNotReject() {
        NativeMetricsSnapshot.set(null);
        assertFalse(check.shouldReject(), "Null snapshot must return false");
    }

    @Test
    void nullStatsInSnapshot_shouldNotReject() {
        NativeMetricsSnapshot.set(new NativeMetricsSnapshot(null, System.currentTimeMillis()));
        assertFalse(check.shouldReject(), "Null stats in snapshot must return false");
    }

    @Test
    void nullCpuRuntimeInStats_shouldNotReject() {
        DataFusionPluginStats stats = buildStats(null);
        NativeMetricsSnapshot.set(new NativeMetricsSnapshot(stats, System.currentTimeMillis()));
        assertFalse(check.shouldReject(), "Null CPU runtime must return false");
    }

    @Test
    void depthAboveThreshold_shouldReject() {
        // workers=4, threshold=40, depth=50 → reject
        DataFusionPluginStats.RuntimeValues cpu = new DataFusionPluginStats.RuntimeValues(4, 0, 0, 0, 50, 0);
        NativeMetricsSnapshot.set(new NativeMetricsSnapshot(buildStats(cpu), System.currentTimeMillis()));
        assertTrue(check.shouldReject(), "depth >= threshold must reject");
    }

    @Test
    void depthBelowThreshold_shouldNotReject() {
        // workers=4, threshold=40, depth=39 → pass
        DataFusionPluginStats.RuntimeValues cpu = new DataFusionPluginStats.RuntimeValues(4, 0, 0, 0, 39, 0);
        NativeMetricsSnapshot.set(new NativeMetricsSnapshot(buildStats(cpu), System.currentTimeMillis()));
        assertFalse(check.shouldReject(), "depth < threshold must not reject");
    }

    @Test
    void depthExactlyAtThreshold_shouldReject() {
        // workers=5, threshold=50, depth=50 → reject (boundary: depth == workers * 10)
        DataFusionPluginStats.RuntimeValues cpu = new DataFusionPluginStats.RuntimeValues(5, 0, 0, 0, 50, 0);
        NativeMetricsSnapshot.set(new NativeMetricsSnapshot(buildStats(cpu), System.currentTimeMillis()));
        assertTrue(check.shouldReject(), "depth == workers_count * 10 exactly must reject");
    }
}
