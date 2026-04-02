/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for {@link NativeMetricsSnapshot}.
 *
 * Feature: tokio-metrics-rejection
 *
 * Validates: Requirements 1.3
 */
public class NativeMetricsSnapshotTests {

    @AfterEach
    void resetSingleton() {
        NativeMetricsSnapshot.set(null);
    }

    @Test
    void getReturnsNullInitially() {
        assertNull(NativeMetricsSnapshot.get(), "INSTANCE should be null before any set() call");
    }

    @Test
    void setGetRoundTripReturnsSameInstance() {
        long[] data = new long[27];
        for (int i = 0; i < 27; i++) {
            data[i] = i + 1;
        }
        DataFusionPluginStats stats = DataFusionPluginStats.decode(data);
        NativeMetricsSnapshot snapshot = new NativeMetricsSnapshot(stats, 12345L);

        NativeMetricsSnapshot.set(snapshot);

        assertSame(snapshot, NativeMetricsSnapshot.get(), "get() should return the exact instance passed to set()");
    }

    @Test
    void snapshotWrapsStatsAndTimestampCorrectly() {
        long[] data = new long[27];
        for (int i = 0; i < 27; i++) {
            data[i] = (i + 1) * 100L;
        }
        DataFusionPluginStats stats = DataFusionPluginStats.decode(data);
        long timestamp = System.currentTimeMillis();

        NativeMetricsSnapshot snapshot = new NativeMetricsSnapshot(stats, timestamp);

        assertNotNull(snapshot.getDataFusionStats(), "dataFusionStats should not be null");
        assertSame(stats, snapshot.getDataFusionStats(), "getDataFusionStats() should return the same stats instance");
        assertEquals(timestamp, snapshot.getLastUpdatedAt(), "getLastUpdatedAt() should return the constructor timestamp");
    }

    @Test
    void snapshotAcceptsNullStats() {
        NativeMetricsSnapshot snapshot = new NativeMetricsSnapshot(null, 99L);

        assertNull(snapshot.getDataFusionStats(), "dataFusionStats should be null when constructed with null");
        assertEquals(99L, snapshot.getLastUpdatedAt());
    }
}
