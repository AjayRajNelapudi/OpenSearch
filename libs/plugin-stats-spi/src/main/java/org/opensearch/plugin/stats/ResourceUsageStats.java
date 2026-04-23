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
 * Resource usage metrics for the native backend.
 *
 * <p>Implements {@link Writeable} for transport serialization and
 * {@link ToXContentFragment} for JSON rendering under the
 * {@code "resource_usage"} key.
 *
 * <p>Currently contains a single metric — {@code nativeMemoryUtilization} —
 * representing the fraction of native (off-heap) memory in use. The value is
 * hardcoded to {@code 0.0} on the Rust side until actual instrumentation is
 * connected.
 */
public class ResourceUsageStats implements Writeable, ToXContentFragment {

    private final double nativeMemoryUtilization;

    /**
     * Construct from a value.
     *
     * @param nativeMemoryUtilization native memory utilization (0.0–1.0)
     */
    public ResourceUsageStats(double nativeMemoryUtilization) {
        this.nativeMemoryUtilization = nativeMemoryUtilization;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public ResourceUsageStats(StreamInput in) throws IOException {
        this.nativeMemoryUtilization = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(nativeMemoryUtilization);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("resource_usage");
        builder.field("native_memory_utilization", nativeMemoryUtilization);
        builder.endObject();
        return builder;
    }

    /**
     * Returns the native memory utilization value.
     */
    public double getNativeMemoryUtilization() {
        return nativeMemoryUtilization;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceUsageStats that = (ResourceUsageStats) o;
        return Double.compare(that.nativeMemoryUtilization, nativeMemoryUtilization) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nativeMemoryUtilization);
    }
}
