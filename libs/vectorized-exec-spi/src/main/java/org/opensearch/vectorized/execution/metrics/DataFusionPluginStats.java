/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Concrete stats for the DataFusion plugin.
 * Contains all runtime (IO + CPU) and task monitor metrics in one class.
 *
 * Lives in the SPI module so that server-side consumers (NodeStats, admission
 * control, thread pool stats) get typed field access directly — same pattern
 * as {@code OsStats} and {@code JvmStats}. No {@code NamedWriteableRegistry}
 * entry is needed.
 *
 * @opensearch.internal
 */
public class DataFusionPluginStats implements PluginStats {

    private static final String IO_RUNTIME = "io_runtime";
    private static final String CPU_RUNTIME = "cpu_runtime";
    private static final String TASK_MONITORS = "task_monitors";
    private static final String QUERY_EXECUTION = "query_execution";
    private static final String STREAM_NEXT = "stream_next";
    private static final String FETCH_PHASE = "fetch_phase";
    private static final String SEGMENT_STATS = "segment_stats";
    private static final String INDEXED_QUERY_EXECUTION = "indexed_query_execution";

    @Nullable
    private final RuntimeValues ioRuntime;
    @Nullable
    private final RuntimeValues cpuRuntime;
    private final TaskMonitorValues queryExecution;
    private final TaskMonitorValues streamNext;
    private final TaskMonitorValues fetchPhase;
    private final TaskMonitorValues segmentStats;
    private final TaskMonitorValues indexedQueryExecution;

    public DataFusionPluginStats(
        @Nullable RuntimeValues ioRuntime,
        @Nullable RuntimeValues cpuRuntime,
        TaskMonitorValues queryExecution,
        TaskMonitorValues streamNext,
        TaskMonitorValues fetchPhase,
        TaskMonitorValues segmentStats,
        TaskMonitorValues indexedQueryExecution
    ) {
        this.ioRuntime = ioRuntime;
        this.cpuRuntime = cpuRuntime;
        this.queryExecution = Objects.requireNonNull(queryExecution);
        this.streamNext = Objects.requireNonNull(streamNext);
        this.fetchPhase = Objects.requireNonNull(fetchPhase);
        this.segmentStats = Objects.requireNonNull(segmentStats);
        this.indexedQueryExecution = Objects.requireNonNull(indexedQueryExecution);
    }

    /**
     * Decodes a flat {@code long[56]} array (from JNI) into a DataFusionPluginStats instance.
     * The array must contain exactly 56 elements laid out as:
     * <pre>
     * [0..17]  io_runtime   (18 fields: workers_count, total_polls_count, total_busy_duration_ms,
     *                         total_overflow_count, global_queue_depth, blocking_queue_depth,
     *                         max_global_queue_depth, p50/p90/p99_global_queue_depth,
     *                         num_alive_tasks, spawned_tasks_count, remote_schedule_count,
     *                         budget_forced_yield_count, num_blocking_threads,
     *                         num_idle_blocking_threads, total_park_count, total_steal_count)
     * [18..35] cpu_runtime  (same 18 fields)
     * [36..39] query_execution          (total_poll_duration_ms, total_scheduled_duration_ms,
     *                                     total_idle_duration_ms, rejected)
     * [40..43] stream_next              (same 4 fields)
     * [44..47] fetch_phase              (same 4 fields)
     * [48..51] segment_stats            (same 4 fields)
     * [52..55] indexed_query_execution  (same 4 fields)
     * </pre>
     *
     * @param data flat long array of 56 elements
     * @return a new DataFusionPluginStats instance
     * @throws IllegalArgumentException if the array is null or not fully consumed
     */
    public static DataFusionPluginStats decode(long[] data) {
        if (data == null) {
            throw new IllegalArgumentException("Cannot decode null array for DataFusionStats");
        }
        ArrayCursor cursor = new ArrayCursor(data);
        RuntimeValues ioRuntime = cursor.read(RuntimeValues::new);
        RuntimeValues cpuRuntime = cursor.read(RuntimeValues::new);
        TaskMonitorValues queryExecution = cursor.read(TaskMonitorValues::new);
        TaskMonitorValues streamNext = cursor.read(TaskMonitorValues::new);
        TaskMonitorValues fetchPhase = cursor.read(TaskMonitorValues::new);
        TaskMonitorValues segmentStats = cursor.read(TaskMonitorValues::new);
        TaskMonitorValues indexedQueryExecution = cursor.read(TaskMonitorValues::new);
        cursor.assertFullyConsumed();
        RuntimeValues cpuOrNull = cpuRuntime.getWorkersCount() == 0 ? null : cpuRuntime;
        return new DataFusionPluginStats(ioRuntime, cpuOrNull, queryExecution, streamNext, fetchPhase, segmentStats, indexedQueryExecution);
    }

    @Nullable
    public RuntimeValues getIoRuntime() {
        return ioRuntime;
    }

    @Nullable
    public RuntimeValues getCpuRuntime() {
        return cpuRuntime;
    }

    public TaskMonitorValues getQueryExecution() {
        return queryExecution;
    }

    public TaskMonitorValues getStreamNext() {
        return streamNext;
    }

    public TaskMonitorValues getFetchPhase() {
        return fetchPhase;
    }

    public TaskMonitorValues getSegmentStats() {
        return segmentStats;
    }

    public TaskMonitorValues getIndexedQueryExecution() {
        return indexedQueryExecution;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionPluginStats that = (DataFusionPluginStats) o;
        return Objects.equals(ioRuntime, that.ioRuntime)
            && Objects.equals(cpuRuntime, that.cpuRuntime)
            && Objects.equals(queryExecution, that.queryExecution)
            && Objects.equals(streamNext, that.streamNext)
            && Objects.equals(fetchPhase, that.fetchPhase)
            && Objects.equals(segmentStats, that.segmentStats)
            && Objects.equals(indexedQueryExecution, that.indexedQueryExecution);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ioRuntime, cpuRuntime, queryExecution, streamNext, fetchPhase, segmentStats, indexedQueryExecution);
    }

    /**
     * Holds all tokio RuntimeMetrics fields for a single runtime.
     * Extends {@link NativeStatsBlock} so it can be decoded from a positional {@code long[]} array.
     */
    public static class RuntimeValues extends NativeStatsBlock implements Writeable {

        /** Offset constants for positional access within this block. */
        public static final int WORKERS_COUNT = 0;
        public static final int TOTAL_POLLS_COUNT = 1;
        public static final int TOTAL_BUSY_DURATION_MS = 2;
        public static final int TOTAL_OVERFLOW_COUNT = 3;
        public static final int GLOBAL_QUEUE_DEPTH = 4;
        public static final int BLOCKING_QUEUE_DEPTH = 5;
        public static final int MAX_GLOBAL_QUEUE_DEPTH = 6;
        public static final int P50_GLOBAL_QUEUE_DEPTH = 7;
        public static final int P90_GLOBAL_QUEUE_DEPTH = 8;
        public static final int P99_GLOBAL_QUEUE_DEPTH = 9;
        public static final int NUM_ALIVE_TASKS = 10;
        public static final int SPAWNED_TASKS_COUNT = 11;
        public static final int REMOTE_SCHEDULE_COUNT = 12;
        public static final int BUDGET_FORCED_YIELD_COUNT = 13;
        public static final int NUM_BLOCKING_THREADS = 14;
        public static final int NUM_IDLE_BLOCKING_THREADS = 15;
        public static final int TOTAL_PARK_COUNT = 16;
        public static final int TOTAL_STEAL_COUNT = 17;
        public static final int TOTAL_NOOP_COUNT = 18;
        public static final int TOTAL_STEAL_OPERATIONS = 19;
        public static final int TOTAL_LOCAL_SCHEDULE_COUNT = 20;
        public static final int TOTAL_LOCAL_QUEUE_DEPTH = 21;
        public static final int SIZE = 22;

        /**
         * Creates a RuntimeValues view over a slice of the given array.
         *
         * @param data   the source array (typically the full JNI payload)
         * @param offset start position within {@code data}
         */
        public RuntimeValues(long[] data, int offset) {
            super(data, offset, SIZE);
        }

        /**
         * Read from a stream.
         */
        public RuntimeValues(StreamInput in) throws IOException {
            super(readFromStream(in), 0, SIZE);
        }

        private static long[] readFromStream(StreamInput in) throws IOException {
            long[] arr = new long[SIZE];
            for (int i = 0; i < SIZE; i++) {
                arr[i] = in.readVLong();
            }
            return arr;
        }

        @Override
        public int size() {
            return SIZE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            for (int i = 0; i < SIZE; i++) {
                out.writeVLong(get(i));
            }
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("workers_count", get(WORKERS_COUNT));
            builder.field("total_polls_count", get(TOTAL_POLLS_COUNT));
            builder.field("total_busy_duration_ms", get(TOTAL_BUSY_DURATION_MS));
            builder.field("total_overflow_count", get(TOTAL_OVERFLOW_COUNT));
            builder.field("global_queue_depth", get(GLOBAL_QUEUE_DEPTH));
            builder.field("blocking_queue_depth", get(BLOCKING_QUEUE_DEPTH));
            builder.field("max_global_queue_depth", get(MAX_GLOBAL_QUEUE_DEPTH));
            builder.field("p50_global_queue_depth", get(P50_GLOBAL_QUEUE_DEPTH));
            builder.field("p90_global_queue_depth", get(P90_GLOBAL_QUEUE_DEPTH));
            builder.field("p99_global_queue_depth", get(P99_GLOBAL_QUEUE_DEPTH));
            builder.field("num_alive_tasks", get(NUM_ALIVE_TASKS));
            builder.field("spawned_tasks_count", get(SPAWNED_TASKS_COUNT));
            builder.field("remote_schedule_count", get(REMOTE_SCHEDULE_COUNT));
            builder.field("budget_forced_yield_count", get(BUDGET_FORCED_YIELD_COUNT));
            builder.field("num_blocking_threads", get(NUM_BLOCKING_THREADS));
            builder.field("num_idle_blocking_threads", get(NUM_IDLE_BLOCKING_THREADS));
            builder.field("total_park_count", get(TOTAL_PARK_COUNT));
            builder.field("total_steal_count", get(TOTAL_STEAL_COUNT));
            builder.field("total_noop_count", get(TOTAL_NOOP_COUNT));
            builder.field("total_steal_operations", get(TOTAL_STEAL_OPERATIONS));
            builder.field("total_local_schedule_count", get(TOTAL_LOCAL_SCHEDULE_COUNT));
            builder.field("total_local_queue_depth", get(TOTAL_LOCAL_QUEUE_DEPTH));
        }

        public long getWorkersCount() { return get(WORKERS_COUNT); }
        public long getTotalPollsCount() { return get(TOTAL_POLLS_COUNT); }
        public long getTotalBusyDurationMs() { return get(TOTAL_BUSY_DURATION_MS); }
        public long getTotalOverflowCount() { return get(TOTAL_OVERFLOW_COUNT); }
        public long getGlobalQueueDepth() { return get(GLOBAL_QUEUE_DEPTH); }
        public long getBlockingQueueDepth() { return get(BLOCKING_QUEUE_DEPTH); }
        public long getMaxGlobalQueueDepth() { return get(MAX_GLOBAL_QUEUE_DEPTH); }
        public long getP50GlobalQueueDepth() { return get(P50_GLOBAL_QUEUE_DEPTH); }
        public long getP90GlobalQueueDepth() { return get(P90_GLOBAL_QUEUE_DEPTH); }
        public long getP99GlobalQueueDepth() { return get(P99_GLOBAL_QUEUE_DEPTH); }
        public long getNumAliveTasks() { return get(NUM_ALIVE_TASKS); }
        public long getSpawnedTasksCount() { return get(SPAWNED_TASKS_COUNT); }
        public long getRemoteScheduleCount() { return get(REMOTE_SCHEDULE_COUNT); }
        public long getBudgetForcedYieldCount() { return get(BUDGET_FORCED_YIELD_COUNT); }
        public long getNumBlockingThreads() { return get(NUM_BLOCKING_THREADS); }
        public long getNumIdleBlockingThreads() { return get(NUM_IDLE_BLOCKING_THREADS); }
        public long getTotalParkCount() { return get(TOTAL_PARK_COUNT); }
        public long getTotalStealCount() { return get(TOTAL_STEAL_COUNT); }
        public long getTotalNoopCount() { return get(TOTAL_NOOP_COUNT); }
        public long getTotalStealOperations() { return get(TOTAL_STEAL_OPERATIONS); }
        public long getTotalLocalScheduleCount() { return get(TOTAL_LOCAL_SCHEDULE_COUNT); }
        public long getTotalLocalQueueDepth() { return get(TOTAL_LOCAL_QUEUE_DEPTH); }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RuntimeValues that = (RuntimeValues) o;
            for (int i = 0; i < SIZE; i++) {
                if (get(i) != that.get(i)) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            long result = 1;
            for (int i = 0; i < SIZE; i++) {
                result = 31 * result + get(i);
            }
            return Long.hashCode(result);
        }
    }

    /**
     * Holds the 4 actionable fields for a single task monitor: three duration
     * metrics plus a rejection counter.
     * Extends {@link NativeStatsBlock} so it can be decoded from a positional {@code long[]} array.
     */
    public static class TaskMonitorValues extends NativeStatsBlock implements Writeable {

        /** Offset constants for positional access within this block. */
        public static final int TOTAL_POLL_DURATION_MS = 0;
        public static final int TOTAL_SCHEDULED_DURATION_MS = 1;
        public static final int TOTAL_IDLE_DURATION_MS = 2;
        public static final int REJECTED = 3;
        public static final int INSTRUMENTED_COUNT = 4;
        public static final int DROPPED_COUNT = 5;
        public static final int FIRST_POLL_COUNT = 6;
        public static final int TOTAL_FIRST_POLL_DELAY_MS = 7;
        public static final int TOTAL_IDLED_COUNT = 8;
        public static final int TOTAL_SCHEDULED_COUNT = 9;
        public static final int TOTAL_POLL_COUNT = 10;
        public static final int TOTAL_FAST_POLL_COUNT = 11;
        public static final int TOTAL_FAST_POLL_DURATION_MS = 12;
        public static final int TOTAL_SLOW_POLL_COUNT = 13;
        public static final int TOTAL_SLOW_POLL_DURATION_MS = 14;
        public static final int TOTAL_SHORT_DELAY_COUNT = 15;
        public static final int TOTAL_LONG_DELAY_COUNT = 16;
        public static final int TOTAL_SHORT_DELAY_DURATION_MS = 17;
        public static final int TOTAL_LONG_DELAY_DURATION_MS = 18;
        public static final int SIZE = 19;

        static final TaskMonitorValues EMPTY = new TaskMonitorValues(new long[SIZE], 0);

        /**
         * Creates a TaskMonitorValues view over a slice of the given array.
         *
         * @param data   the source array (typically the full JNI payload)
         * @param offset start position within {@code data}
         */
        public TaskMonitorValues(long[] data, int offset) {
            super(data, offset, SIZE);
        }

        /**
         * Read from a stream.
         */
        public TaskMonitorValues(StreamInput in) throws IOException {
            super(readFromStream(in, SIZE), 0, SIZE);
        }

        private static long[] readFromStream(StreamInput in, int size) throws IOException {
            long[] arr = new long[size];
            for (int i = 0; i < size; i++) {
                arr[i] = in.readVLong();
            }
            return arr;
        }

        @Override
        public int size() {
            return SIZE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            for (int i = 0; i < SIZE; i++) {
                out.writeVLong(get(i));
            }
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.field("total_poll_duration_ms", get(TOTAL_POLL_DURATION_MS));
            builder.field("total_scheduled_duration_ms", get(TOTAL_SCHEDULED_DURATION_MS));
            builder.field("total_idle_duration_ms", get(TOTAL_IDLE_DURATION_MS));
            builder.field("rejected", get(REJECTED));
            builder.field("instrumented_count", get(INSTRUMENTED_COUNT));
            builder.field("dropped_count", get(DROPPED_COUNT));
            builder.field("first_poll_count", get(FIRST_POLL_COUNT));
            builder.field("total_first_poll_delay_ms", get(TOTAL_FIRST_POLL_DELAY_MS));
            builder.field("total_idled_count", get(TOTAL_IDLED_COUNT));
            builder.field("total_scheduled_count", get(TOTAL_SCHEDULED_COUNT));
            builder.field("total_poll_count", get(TOTAL_POLL_COUNT));
            builder.field("total_fast_poll_count", get(TOTAL_FAST_POLL_COUNT));
            builder.field("total_fast_poll_duration_ms", get(TOTAL_FAST_POLL_DURATION_MS));
            builder.field("total_slow_poll_count", get(TOTAL_SLOW_POLL_COUNT));
            builder.field("total_slow_poll_duration_ms", get(TOTAL_SLOW_POLL_DURATION_MS));
            builder.field("total_short_delay_count", get(TOTAL_SHORT_DELAY_COUNT));
            builder.field("total_long_delay_count", get(TOTAL_LONG_DELAY_COUNT));
            builder.field("total_short_delay_duration_ms", get(TOTAL_SHORT_DELAY_DURATION_MS));
            builder.field("total_long_delay_duration_ms", get(TOTAL_LONG_DELAY_DURATION_MS));
        }

        public long getTotalPollDurationMs() { return get(TOTAL_POLL_DURATION_MS); }
        public long getTotalScheduledDurationMs() { return get(TOTAL_SCHEDULED_DURATION_MS); }
        public long getTotalIdleDurationMs() { return get(TOTAL_IDLE_DURATION_MS); }
        public long getRejected() { return get(REJECTED); }
        public long getInstrumentedCount() { return get(INSTRUMENTED_COUNT); }
        public long getDroppedCount() { return get(DROPPED_COUNT); }
        public long getFirstPollCount() { return get(FIRST_POLL_COUNT); }
        public long getTotalFirstPollDelayMs() { return get(TOTAL_FIRST_POLL_DELAY_MS); }
        public long getTotalIdledCount() { return get(TOTAL_IDLED_COUNT); }
        public long getTotalScheduledCount() { return get(TOTAL_SCHEDULED_COUNT); }
        public long getTotalPollCount() { return get(TOTAL_POLL_COUNT); }
        public long getTotalFastPollCount() { return get(TOTAL_FAST_POLL_COUNT); }
        public long getTotalFastPollDurationMs() { return get(TOTAL_FAST_POLL_DURATION_MS); }
        public long getTotalSlowPollCount() { return get(TOTAL_SLOW_POLL_COUNT); }
        public long getTotalSlowPollDurationMs() { return get(TOTAL_SLOW_POLL_DURATION_MS); }
        public long getTotalShortDelayCount() { return get(TOTAL_SHORT_DELAY_COUNT); }
        public long getTotalLongDelayCount() { return get(TOTAL_LONG_DELAY_COUNT); }
        public long getTotalShortDelayDurationMs() { return get(TOTAL_SHORT_DELAY_DURATION_MS); }
        public long getTotalLongDelayDurationMs() { return get(TOTAL_LONG_DELAY_DURATION_MS); }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TaskMonitorValues that = (TaskMonitorValues) o;
            for (int i = 0; i < SIZE; i++) {
                if (get(i) != that.get(i)) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            long result = 1;
            for (int i = 0; i < SIZE; i++) {
                result = 31 * result + get(i);
            }
            return Long.hashCode(result);
        }
    }
}
