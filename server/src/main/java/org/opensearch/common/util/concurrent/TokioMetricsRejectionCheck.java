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
 * Rejects tasks when either the Tokio CPU or IO runtime's global queue depth
 * exceeds {@code workers_count * QUEUE_DEPTH_MULTIPLIER}. Reads from the cached
 * {@link NativeMetricsSnapshot} — a single volatile read, no JNI.
 *
 * @opensearch.internal
 */
final class TokioMetricsRejectionCheck implements QueueRejectionCheck {

    /** Multiplier applied to workers_count to compute the queue depth threshold. TBD via load testing. */
    static final long QUEUE_DEPTH_MULTIPLIER = 10;

    @Override
    public boolean shouldReject() {
        NativeMetricsSnapshot snapshot = NativeMetricsSnapshot.get();
        if (snapshot == null) {
            return false;
        }
        DataFusionPluginStats stats = snapshot.getDataFusionStats();
        if (stats == null) {
            return false;
        }
        return isRuntimeSaturated(stats.getCpuRuntime()) || isRuntimeSaturated(stats.getIoRuntime());
    }

    private static boolean isRuntimeSaturated(DataFusionPluginStats.RuntimeValues runtime) {
        if (runtime == null) {
            return false;
        }
        long depth = runtime.getGlobalQueueDepth();
        long threshold = runtime.getWorkersCount() * QUEUE_DEPTH_MULTIPLIER;
        return depth >= threshold;
    }
}
