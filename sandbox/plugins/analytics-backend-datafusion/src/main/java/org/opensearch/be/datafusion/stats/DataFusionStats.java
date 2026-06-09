/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Top-level stats container for the DataFusion backend.
 *
 * <p>Implements {@link Writeable} for transport serialization,
 * {@link Writeable} for transport serialization, and {@link ToXContentFragment}
 * for JSON rendering.
 *
 * <p>Composes {@link NativeExecutorsStats} rather than duplicating its fields,
 * making it extensible for future metric categories (e.g. MemoryPoolStats).
 * No inner classes — {@code RuntimeMetrics} and {@code TaskMonitorStats} belong
 * to {@link NativeExecutorsStats}.
 */
public class DataFusionStats implements Writeable, ToXContentFragment {

    private final NativeExecutorsStats nativeExecutorsStats; // nullable
    private final PartitionGateStats fragmentExecutorGateStats; // nullable

    /**
     * Construct from components.
     *
     * @param nativeExecutorsStats  the native executor metrics (nullable)
     * @param fragmentExecutorGateStats     the datanode partition gate metrics (nullable)
     */
    public DataFusionStats(NativeExecutorsStats nativeExecutorsStats, PartitionGateStats fragmentExecutorGateStats) {
        this.nativeExecutorsStats = nativeExecutorsStats;
        this.fragmentExecutorGateStats = fragmentExecutorGateStats;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public DataFusionStats(StreamInput in) throws IOException {
        this.nativeExecutorsStats = in.readOptionalWriteable(NativeExecutorsStats::new);
        this.fragmentExecutorGateStats = in.readOptionalWriteable(PartitionGateStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(nativeExecutorsStats);
        out.writeOptionalWriteable(fragmentExecutorGateStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (nativeExecutorsStats != null) {
            nativeExecutorsStats.toXContent(builder, params);
        }
        if (fragmentExecutorGateStats != null) {
            fragmentExecutorGateStats.toXContent(builder, params);
        }
        return builder;
    }

    /**
     * Returns the native executor metrics, or {@code null} if absent.
     */
    public NativeExecutorsStats getNativeExecutorsStats() {
        return nativeExecutorsStats;
    }

    /**
     * Returns the datanode partition gate metrics, or {@code null} if absent.
     */
    public PartitionGateStats getFragmentExecutorGateStats() {
        return fragmentExecutorGateStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionStats that = (DataFusionStats) o;
        return Objects.equals(nativeExecutorsStats, that.nativeExecutorsStats)
            && Objects.equals(fragmentExecutorGateStats, that.fragmentExecutorGateStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativeExecutorsStats, fragmentExecutorGateStats);
    }
}
