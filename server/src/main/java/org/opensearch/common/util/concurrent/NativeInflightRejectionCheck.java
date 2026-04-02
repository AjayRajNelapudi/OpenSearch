/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTrackerRegistry;

import java.util.Collection;

/**
 * Rejects tasks when the total native in-flight count exceeds a
 * hardcoded concurrency limit. Reads from the static
 * {@link NativeExecutorTrackerRegistry} — no queue state needed.
 * The limit is independent of queue capacity and will be determined
 * via load testing.
 *
 * @opensearch.internal
 */
final class NativeInflightRejectionCheck implements QueueRejectionCheck {

    private final int maxNativeInFlight;

    NativeInflightRejectionCheck(int maxNativeInFlight) {
        this.maxNativeInFlight = maxNativeInFlight;
    }

    @Override
    public boolean shouldReject() {
        Collection<NativeExecutorTracker> trackers = NativeExecutorTrackerRegistry.getAll();
        if (trackers.isEmpty()) {
            return false;
        }
        int total = 0;
        for (NativeExecutorTracker tracker : trackers) {
            total += tracker.getNativeInFlight();
        }
        return total >= maxNativeInFlight;
    }
}
