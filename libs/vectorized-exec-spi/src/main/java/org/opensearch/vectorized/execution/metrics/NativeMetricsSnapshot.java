/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import org.opensearch.common.Nullable;

/**
 * Shared snapshot of native runtime metrics. Written by
 * {@code NativeMetricsCollectorService} on a background thread and read by
 * {@code TokioMetricsRejectionCheck} on the queue's offer path.
 * <p>
 * The static volatile {@link #INSTANCE} field provides a single-writer /
 * multi-reader publish point — no locks required. Each snapshot is immutable
 * once constructed; the volatile reference guarantees happens-before between
 * the writer and all readers.
 * <p>
 * Designed for extensibility: future native runtimes (Rayon, Parquet) will
 * add additional fields to this class.
 *
 * @opensearch.internal
 */
public final class NativeMetricsSnapshot {

    private static volatile NativeMetricsSnapshot INSTANCE;

    @Nullable
    private final DataFusionPluginStats dataFusionStats;

    private final long lastUpdatedAt;

    public NativeMetricsSnapshot(@Nullable DataFusionPluginStats dataFusionStats, long lastUpdatedAt) {
        this.dataFusionStats = dataFusionStats;
        this.lastUpdatedAt = lastUpdatedAt;
    }

    @Nullable
    public DataFusionPluginStats getDataFusionStats() {
        return dataFusionStats;
    }

    public long getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public static NativeMetricsSnapshot get() {
        return INSTANCE;
    }

    public static void set(NativeMetricsSnapshot snapshot) {
        INSTANCE = snapshot;
    }
}
