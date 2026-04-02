/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;
import org.opensearch.vectorized.execution.metrics.MetricProvider;
import org.opensearch.vectorized.execution.metrics.NativeMetricsSnapshot;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based and unit tests for {@link NativeMetricsCollectorService}.
 */
public class NativeMetricsCollectorServiceTests {

    @AfterEach
    void resetSnapshot() {
        NativeMetricsSnapshot.set(null);
    }

    // --- Generators ---

    @Provide
    Arbitrary<long[]> longArray27() {
        return Arbitraries.longs().between(1, Long.MAX_VALUE / 2).array(long[].class).ofSize(27);
    }

    // --- Property Tests ---

    // Feature: tokio-metrics-rejection, Property 1: Collection round-trip
    // **Validates: Requirements 1.3**
    @Property(tries = 100)
    void collectionRoundTripPreservesStatsInstance(@ForAll("longArray27") long[] raw) {
        DataFusionPluginStats stats = DataFusionPluginStats.decode(raw);
        MetricProvider<DataFusionPluginStats> provider = () -> stats;

        NativeMetricsCollectorService service = new NativeMetricsCollectorService(null);
        service.registerMetricProvider(provider);
        service.collectMetrics();

        NativeMetricsSnapshot snapshot = NativeMetricsSnapshot.get();
        assertNotNull(snapshot, "Snapshot should be set after collection");
        assertSame(stats, snapshot.getDataFusionStats(),
            "Snapshot should contain the exact same DataFusionPluginStats instance returned by the provider");

        // Reset for next iteration
        NativeMetricsSnapshot.set(null);
    }

    // Feature: tokio-metrics-rejection, Property 2: Error retention preserves cached snapshot
    // **Validates: Requirements 1.5**
    @Property(tries = 100)
    void errorRetentionPreservesCachedSnapshot(@ForAll("longArray27") long[] raw) {
        DataFusionPluginStats initialStats = DataFusionPluginStats.decode(raw);
        NativeMetricsSnapshot initialSnapshot = new NativeMetricsSnapshot(initialStats, System.currentTimeMillis());
        NativeMetricsSnapshot.set(initialSnapshot);

        MetricProvider<DataFusionPluginStats> throwingProvider = () -> {
            throw new RuntimeException("simulated JNI failure");
        };

        NativeMetricsCollectorService service = new NativeMetricsCollectorService(null);
        service.registerMetricProvider(throwingProvider);
        service.collectMetrics();

        assertSame(initialSnapshot, NativeMetricsSnapshot.get(),
            "Snapshot should be unchanged after provider throws an exception");

        // Reset for next iteration
        NativeMetricsSnapshot.set(null);
    }

    // --- Unit Tests ---

    @Test
    void noProviderRegistered_snapshotStaysNull() {
        NativeMetricsCollectorService service = new NativeMetricsCollectorService(null);
        service.collectMetrics();

        assertNull(NativeMetricsSnapshot.get(),
            "Snapshot should remain null when no provider is registered");
    }

    @Test
    void providerRegistered_snapshotUpdatedWithNewStats() {
        long[] raw = new long[27];
        for (int i = 0; i < 27; i++) {
            raw[i] = i + 1;
        }
        DataFusionPluginStats stats = DataFusionPluginStats.decode(raw);
        MetricProvider<DataFusionPluginStats> provider = () -> stats;

        NativeMetricsCollectorService service = new NativeMetricsCollectorService(null);
        service.registerMetricProvider(provider);
        service.collectMetrics();

        NativeMetricsSnapshot snapshot = NativeMetricsSnapshot.get();
        assertNotNull(snapshot, "Snapshot should be set after collection");
        assertSame(stats, snapshot.getDataFusionStats());
        assertTrue(snapshot.getLastUpdatedAt() > 0, "Timestamp should be positive");
    }

    @Test
    void secondProviderRegistrationReplacesFirst() {
        long[] raw1 = new long[27];
        long[] raw2 = new long[27];
        for (int i = 0; i < 27; i++) {
            raw1[i] = i + 1;
            raw2[i] = i + 100;
        }
        DataFusionPluginStats stats1 = DataFusionPluginStats.decode(raw1);
        DataFusionPluginStats stats2 = DataFusionPluginStats.decode(raw2);

        NativeMetricsCollectorService service = new NativeMetricsCollectorService(null);

        // Register first provider and collect
        service.registerMetricProvider(() -> stats1);
        service.collectMetrics();
        assertSame(stats1, NativeMetricsSnapshot.get().getDataFusionStats());

        // Register second provider (replaces first) and collect
        service.registerMetricProvider(() -> stats2);
        service.collectMetrics();
        assertSame(stats2, NativeMetricsSnapshot.get().getDataFusionStats());
    }

    @Test
    void rejectionCounterStartsAtZeroAndIncrements() {
        NativeMetricsCollectorService service = new NativeMetricsCollectorService(null);
        assertEquals(0, service.getRejectionCount());

        service.incrementRejectionCount();
        assertEquals(1, service.getRejectionCount());

        service.incrementRejectionCount();
        service.incrementRejectionCount();
        assertEquals(3, service.getRejectionCount());
    }
}
