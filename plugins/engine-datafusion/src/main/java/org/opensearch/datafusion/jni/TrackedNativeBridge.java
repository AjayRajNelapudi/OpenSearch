/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.jni;

import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.FileStats;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTrackerRegistry;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Backpressure wrapper around {@link NativeBridge} JNI dispatch methods.
 * Each method acquires/releases the corresponding {@link NativeExecutorTracker}.
 * <p>
 * Async methods ({@code executeQueryPhaseAsync}, {@code executeIndexedQueryAsync}) use
 * ActionListener wrapping with an {@link AtomicBoolean} guard to ensure {@code release()}
 * is called exactly once.
 * <p>
 * Sync/callback methods ({@code streamNext}, {@code executeFetchPhase},
 * {@code fetchSegmentStats}) use try/finally wrapping.
 * <p>
 * Rejection is not performed here — it happens at the queue level via
 * {@code NativeInflightAwareQueue}, which inflates the search pool queue's
 * apparent size by the sum of all trackers' in-flight counts.
 *
 * @opensearch.internal
 */
public final class TrackedNativeBridge {

    private TrackedNativeBridge() {}

    // --- Async: ActionListener wrapping with AtomicBoolean guard ---

    /**
     * Dispatch a query-phase execution with per-operation in-flight tracking.
     */
    public static void executeQueryPhaseWithTracking(
        long readerPtr,
        String tableName,
        byte[] plan,
        boolean explain,
        int partitionCount,
        long runtimePtr,
        ActionListener<Long> listener
    ) {
        NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("query_execution");
        tracker.acquire();
        final AtomicBoolean released = new AtomicBoolean(false);
        try {
            NativeBridge.executeQueryPhaseAsync(
                readerPtr, tableName, plan, explain, partitionCount, runtimePtr,
                wrapListener(tracker, released, listener)
            );
        } catch (Exception e) {
            if (released.compareAndSet(false, true)) {
                tracker.release();
            }
            listener.onFailure(e);
        }
    }

    /**
     * Dispatch an indexed query execution with per-operation in-flight tracking.
     */
    public static void executeIndexedQueryWithTracking(
        long weightPtr,
        long[] segmentMaxDocs,
        String[] parquetPaths,
        String tableName,
        byte[] substraitBytes,
        int numPartitions,
        int bitsetMode,
        boolean isQueryPlanExplainEnabled,
        long runtimePtr,
        ActionListener<Long> listener
    ) {
        NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("indexed_query_execution");
        tracker.acquire();
        final AtomicBoolean released = new AtomicBoolean(false);
        try {
            NativeBridge.executeIndexedQueryAsync(
                weightPtr, segmentMaxDocs, parquetPaths, tableName, substraitBytes,
                numPartitions, bitsetMode, isQueryPlanExplainEnabled, runtimePtr,
                wrapListener(tracker, released, listener)
            );
        } catch (Exception e) {
            if (released.compareAndSet(false, true)) {
                tracker.release();
            }
            listener.onFailure(e);
        }
    }

    // --- Sync/callback: try/finally wrapping ---

    /**
     * Dispatch a stream-next operation with per-operation in-flight tracking.
     */
    public static void streamNextWithTracking(
        long runtime,
        long stream,
        ActionListener<Long> listener
    ) {
        NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("stream_next");
        tracker.acquire();
        try {
            NativeBridge.streamNext(runtime, stream, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            tracker.release();
        }
    }

    /**
     * Dispatch a fetch-phase execution with per-operation in-flight tracking.
     */
    public static long executeFetchPhaseWithTracking(
        long readerPtr,
        long[] rowIds,
        String[] includeFields,
        String[] excludeFields,
        long runtimePtr
    ) {
        NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("fetch_phase");
        tracker.acquire();
        try {
            return NativeBridge.executeFetchPhase(
                readerPtr, rowIds, includeFields, excludeFields, runtimePtr
            );
        } finally {
            tracker.release();
        }
    }

    /**
     * Dispatch a fetch-segment-stats operation with per-operation in-flight tracking.
     */
    public static void fetchSegmentStatsWithTracking(
        long readerPtr,
        ActionListener<Map<String, FileStats>> listener
    ) {
        NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("segment_stats");
        tracker.acquire();
        try {
            NativeBridge.fetchSegmentStats(readerPtr, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            tracker.release();
        }
    }

    // --- Shared listener wrapper ---

    /**
     * Wraps a delegate {@link ActionListener} so that {@code tracker.release()} is called
     * exactly once, guarded by the provided {@link AtomicBoolean}.
     */
    private static ActionListener<Long> wrapListener(
        NativeExecutorTracker tracker,
        AtomicBoolean released,
        ActionListener<Long> delegate
    ) {
        return new ActionListener<Long>() {
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
    }
}
