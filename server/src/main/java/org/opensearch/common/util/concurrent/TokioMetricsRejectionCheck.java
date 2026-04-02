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

/**
 * Rejects tasks when the Tokio CPU runtime's global queue depth
 * exceeds {@code workers_count * 10}. Reads from the cached
 * {@link NativeMetricsSnapshot} — a single volatile read, no JNI.
 *
 * @opensearch.internal
 */
final class TokioMetricsRejectionCheck implements QueueRejectionCheck {

    @Override
    public boolean shouldReject() {
        NativeMetricsSnapshot snapshot = NativeMetricsSnapshot.get();
        if (snapshot == null) {
            return false;
        }
        DataFusionPluginStats stats = snapshot.getDataFusionStats();
        if (stats == null || stats.getCpuRuntime() == null) {
            return false;
        }
        long depth = stats.getCpuRuntime().getGlobalQueueDepth();
        long threshold = 2;
        return depth >= threshold;
    }
}
