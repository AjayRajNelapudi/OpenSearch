/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats.RuntimeValues;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats.TaskMonitorValues;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Server-side Writeable + ToXContentFragment wrapper around the
 * {@link DataFusionPluginStats} POJO.  Handles transport serialization
 * and JSON rendering for the {@code native_executors} section of the
 * nodes-stats API response.
 *
 * Carries per-operation native in-flight counters when the DataFusion
 * plugin is installed and trackers are available.
 *
 * @opensearch.internal
 */
public class NativeExecutorsStats implements Writeable, ToXContentFragment {

    private final DataFusionPluginStats dataFusionPluginStats;

    /** Per-operation tracker snapshots: name → [inFlight, acquired]. */
    private final Map<String, long[]> perOperationInflight;

    /** Cumulative count of rejections due to backpressure checks. */
    private final long rejections;

    /**
     * Construct stats with per-operation tracker values and rejection counter.
     */
    public NativeExecutorsStats(DataFusionPluginStats stats, List<NativeExecutorTracker> trackers, long rejections) {
        this.dataFusionPluginStats = Objects.requireNonNull(stats);
        if (trackers != null && !trackers.isEmpty()) {
            this.perOperationInflight = new LinkedHashMap<>(trackers.size());
            for (NativeExecutorTracker t : trackers) {
                perOperationInflight.put(t.getName(), new long[] { t.getNativeInFlight(), t.getNativeAcquired() });
            }
        } else {
            this.perOperationInflight = Collections.emptyMap();
        }
        this.rejections = rejections;
    }

    /**
     * Construct stats with per-operation tracker values (zero rejections).
     */
    public NativeExecutorsStats(DataFusionPluginStats stats, List<NativeExecutorTracker> trackers) {
        this(stats, trackers, 0);
    }

    /**
     * Construct stats without tracker values (zero rejections).
     */
    public NativeExecutorsStats(DataFusionPluginStats stats) {
        this.dataFusionPluginStats = Objects.requireNonNull(stats);
        this.perOperationInflight = Collections.emptyMap();
        this.rejections = 0;
    }

    /**
     * Package-private constructor for deserialization and testing.
     */
    NativeExecutorsStats(DataFusionPluginStats stats, Map<String, long[]> perOperationInflight, long rejections) {
        this.dataFusionPluginStats = Objects.requireNonNull(stats);
        this.perOperationInflight = perOperationInflight != null ? perOperationInflight : Collections.emptyMap();
        this.rejections = rejections;
    }

    /**
     * Package-private constructor for deserialization and testing (zero rejections).
     */
    NativeExecutorsStats(DataFusionPluginStats stats, Map<String, long[]> perOperationInflight) {
        this(stats, perOperationInflight, 0);
    }

    public NativeExecutorsStats(StreamInput in) throws IOException {
        RuntimeValues ioRuntime = in.readOptionalWriteable(RuntimeValues::new);
        RuntimeValues cpuRuntime = in.readOptionalWriteable(RuntimeValues::new);
        TaskMonitorValues queryExecution = new TaskMonitorValues(in);
        TaskMonitorValues streamNext = new TaskMonitorValues(in);
        TaskMonitorValues fetchPhase = new TaskMonitorValues(in);
        TaskMonitorValues segmentStats = new TaskMonitorValues(in);
        TaskMonitorValues indexedQueryExecution = new TaskMonitorValues(in);
        this.dataFusionPluginStats = new DataFusionPluginStats(
            ioRuntime, cpuRuntime, queryExecution, streamNext, fetchPhase, segmentStats, indexedQueryExecution
        );
        int count = in.readVInt();
        if (count > 0) {
            perOperationInflight = new LinkedHashMap<>(count);
            for (int i = 0; i < count; i++) {
                String name = in.readString();
                long inFlight = in.readLong();
                long acquired = in.readLong();
                perOperationInflight.put(name, new long[] { inFlight, acquired });
            }
        } else {
            perOperationInflight = Collections.emptyMap();
        }
        this.rejections = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(dataFusionPluginStats.getIoRuntime());
        out.writeOptionalWriteable(dataFusionPluginStats.getCpuRuntime());
        dataFusionPluginStats.getQueryExecution().writeTo(out);
        dataFusionPluginStats.getStreamNext().writeTo(out);
        dataFusionPluginStats.getFetchPhase().writeTo(out);
        dataFusionPluginStats.getSegmentStats().writeTo(out);
        dataFusionPluginStats.getIndexedQueryExecution().writeTo(out);
        out.writeVInt(perOperationInflight.size());
        for (Map.Entry<String, long[]> entry : perOperationInflight.entrySet()) {
            out.writeString(entry.getKey());
            out.writeLong(entry.getValue()[0]); // inFlight
            out.writeLong(entry.getValue()[1]); // acquired
        }
        out.writeLong(rejections);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        RuntimeValues ioRuntime = dataFusionPluginStats.getIoRuntime();
        if (ioRuntime != null) {
            builder.startObject("io_runtime");
            ioRuntime.toXContent(builder);
            builder.endObject();
        }
        RuntimeValues cpuRuntime = dataFusionPluginStats.getCpuRuntime();
        if (cpuRuntime != null) {
            builder.startObject("cpu_runtime");
            cpuRuntime.toXContent(builder);
            builder.endObject();
        }
        builder.startObject("task_monitors");

        builder.startObject("query_execution");
        dataFusionPluginStats.getQueryExecution().toXContent(builder);
        appendInflight(builder, "query_execution");
        builder.endObject();

        builder.startObject("stream_next");
        dataFusionPluginStats.getStreamNext().toXContent(builder);
        appendInflight(builder, "stream_next");
        builder.endObject();

        builder.startObject("fetch_phase");
        dataFusionPluginStats.getFetchPhase().toXContent(builder);
        appendInflight(builder, "fetch_phase");
        builder.endObject();

        builder.startObject("segment_stats");
        dataFusionPluginStats.getSegmentStats().toXContent(builder);
        appendInflight(builder, "segment_stats");
        builder.endObject();

        builder.startObject("indexed_query_execution");
        dataFusionPluginStats.getIndexedQueryExecution().toXContent(builder);
        appendInflight(builder, "indexed_query_execution");
        builder.endObject();

        builder.endObject(); // end task_monitors

        // Always render tasks section — defaults to zeros when no trackers registered yet
        long totalInFlight = 0;
        long totalAcquired = 0;
        for (long[] vals : perOperationInflight.values()) {
            totalInFlight += vals[0];
            totalAcquired += vals[1];
        }
        builder.startObject("tasks");
        builder.field("total_in_flight", totalInFlight);
        builder.field("total_acquired", totalAcquired);
        builder.field("rejections", rejections);
        builder.endObject();
        return builder;
    }

    private void appendInflight(XContentBuilder builder, String operationName) throws IOException {
        long[] vals = perOperationInflight.get(operationName);
        if (vals != null) {
            builder.field("in_flight", vals[0]);
            builder.field("acquired", vals[1]);
        }
    }

    public DataFusionPluginStats getDataFusionPluginStats() {
        return dataFusionPluginStats;
    }

    /** Returns per-operation in-flight snapshot: name → [inFlight, acquired]. */
    public Map<String, long[]> getPerOperationInflight() {
        return perOperationInflight;
    }

    /** Returns the cumulative rejection count. */
    public long getRejections() {
        return rejections;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NativeExecutorsStats that = (NativeExecutorsStats) o;
        if (!Objects.equals(dataFusionPluginStats, that.dataFusionPluginStats)) return false;
        if (rejections != that.rejections) return false;
        if (perOperationInflight.size() != that.perOperationInflight.size()) return false;
        for (Map.Entry<String, long[]> entry : perOperationInflight.entrySet()) {
            long[] otherVal = that.perOperationInflight.get(entry.getKey());
            if (otherVal == null || !Arrays.equals(entry.getValue(), otherVal)) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(dataFusionPluginStats);
        result = 31 * result + Long.hashCode(rejections);
        for (Map.Entry<String, long[]> entry : perOperationInflight.entrySet()) {
            result = 31 * result + entry.getKey().hashCode();
            result = 31 * result + Arrays.hashCode(entry.getValue());
        }
        return result;
    }
}
