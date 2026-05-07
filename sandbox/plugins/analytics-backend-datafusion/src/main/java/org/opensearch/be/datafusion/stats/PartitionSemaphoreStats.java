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
 * Partition semaphore metrics from the native DataFusion runtime.
 *
 * <p>Exposes 4 metrics:
 * <ul>
 *   <li>{@code max_permits} — total semaphore capacity (immutable after init)</li>
 *   <li>{@code active_permits} — currently held permits</li>
 *   <li>{@code total_wait_duration_ms} — cumulative ms queries spent waiting</li>
 *   <li>{@code total_partitions_started} — cumulative permits granted since startup</li>
 * </ul>
 */
public class PartitionSemaphoreStats implements Writeable, ToXContentFragment {

    /** Total semaphore capacity. */
    public final long maxPermits;
    /** Currently held permits. */
    public final long activePermits;
    /** Cumulative milliseconds queries spent waiting for permits. */
    public final long totalWaitDurationMs;
    /** Cumulative count of permits granted since startup. */
    public final long totalPartitionsStarted;

    /**
     * Construct from explicit field values.
     *
     * @param maxPermits             total semaphore capacity
     * @param activePermits          currently held permits
     * @param totalWaitDurationMs    cumulative wait time in milliseconds
     * @param totalPartitionsStarted cumulative permits granted
     */
    public PartitionSemaphoreStats(long maxPermits, long activePermits, long totalWaitDurationMs, long totalPartitionsStarted) {
        this.maxPermits = maxPermits;
        this.activePermits = activePermits;
        this.totalWaitDurationMs = totalWaitDurationMs;
        this.totalPartitionsStarted = totalPartitionsStarted;
    }

    /**
     * Deserialize from stream.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public PartitionSemaphoreStats(StreamInput in) throws IOException {
        this.maxPermits = in.readVLong();
        this.activePermits = in.readVLong();
        this.totalWaitDurationMs = in.readVLong();
        this.totalPartitionsStarted = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(maxPermits);
        out.writeVLong(activePermits);
        out.writeVLong(totalWaitDurationMs);
        out.writeVLong(totalPartitionsStarted);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("partition_semaphore");
        builder.field("max_permits", maxPermits);
        builder.field("active_permits", activePermits);
        builder.field("total_wait_duration_ms", totalWaitDurationMs);
        builder.field("total_partitions_started", totalPartitionsStarted);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartitionSemaphoreStats that = (PartitionSemaphoreStats) o;
        return maxPermits == that.maxPermits
            && activePermits == that.activePermits
            && totalWaitDurationMs == that.totalWaitDurationMs
            && totalPartitionsStarted == that.totalPartitionsStarted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxPermits, activePermits, totalWaitDurationMs, totalPartitionsStarted);
    }
}
