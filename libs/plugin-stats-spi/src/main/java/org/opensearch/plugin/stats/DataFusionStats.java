/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

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
 * <p>Implements {@link PluginStats} for Mustang Stats Framework compatibility,
 * {@link Writeable} for transport serialization, and {@link ToXContentFragment}
 * for JSON rendering.
 *
 * <p>Composes {@link NativeExecutorsStats} and optionally {@link ResourceUsageStats}
 * rather than duplicating their fields, making it extensible for future metric
 * categories. No inner classes — {@code RuntimeMetrics} and {@code TaskMonitorStats}
 * belong to {@link NativeExecutorsStats}.
 */
public class DataFusionStats implements PluginStats, Writeable, ToXContentFragment {

    private final NativeExecutorsStats nativeExecutorsStats; // nullable
    private final ResourceUsageStats resourceUsageStats; // nullable

    /**
     * Construct from native executor stats only (backward-compatible).
     *
     * @param nativeExecutorsStats the native executor metrics (nullable)
     */
    public DataFusionStats(NativeExecutorsStats nativeExecutorsStats) {
        this(nativeExecutorsStats, null);
    }

    /**
     * Construct from components.
     *
     * @param nativeExecutorsStats the native executor metrics (nullable)
     * @param resourceUsageStats   the resource usage metrics (nullable)
     */
    public DataFusionStats(NativeExecutorsStats nativeExecutorsStats, ResourceUsageStats resourceUsageStats) {
        this.nativeExecutorsStats = nativeExecutorsStats;
        this.resourceUsageStats = resourceUsageStats;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public DataFusionStats(StreamInput in) throws IOException {
        this.nativeExecutorsStats = in.readOptionalWriteable(NativeExecutorsStats::new);
        this.resourceUsageStats = in.readOptionalWriteable(ResourceUsageStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(nativeExecutorsStats);
        out.writeOptionalWriteable(resourceUsageStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (nativeExecutorsStats != null) {
            nativeExecutorsStats.toXContent(builder, params);
        }
        if (resourceUsageStats != null) {
            resourceUsageStats.toXContent(builder, params);
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
     * Returns the resource usage metrics, or {@code null} if absent.
     */
    public ResourceUsageStats getResourceUsageStats() {
        return resourceUsageStats;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataFusionStats that = (DataFusionStats) o;
        return Objects.equals(nativeExecutorsStats, that.nativeExecutorsStats)
            && Objects.equals(resourceUsageStats, that.resourceUsageStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativeExecutorsStats, resourceUsageStats);
    }
}
