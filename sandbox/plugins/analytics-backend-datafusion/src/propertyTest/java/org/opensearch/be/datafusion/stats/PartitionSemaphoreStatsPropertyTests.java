/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Property-based tests for {@link PartitionSemaphoreStats} serialization round-trip.
 *
 * <p>Validates Property 4 from the query-partition-semaphore design:
 * For any valid PartitionSemaphoreStats instance (4 non-negative i64 values),
 * serializing via writeTo(StreamOutput) and deserializing via new PartitionSemaphoreStats(StreamInput)
 * SHALL produce an equal instance.
 */
public class PartitionSemaphoreStatsPropertyTests {

    @Provide
    Arbitrary<PartitionSemaphoreStats> partitionSemaphoreStats() {
        Arbitrary<Long> nonNegLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(nonNegLong, nonNegLong, nonNegLong, nonNegLong)
            .as(PartitionSemaphoreStats::new);
    }

    /**
     * Property 4: PartitionSemaphoreStats StreamOutput/StreamInput round trip.
     *
     * <p>For any valid PartitionSemaphoreStats instance (4 non-negative i64 values),
     * serializing via writeTo(StreamOutput) and deserializing via new PartitionSemaphoreStats(StreamInput)
     * SHALL produce an equal instance.
     *
     * <p>Validates: Requirement 11.3
     */
    @Property(tries = 100)
    @Tag("Feature: query-partition-semaphore, Property 4: PartitionSemaphoreStats StreamOutput/StreamInput round trip")
    void writeableRoundTrip(@ForAll("partitionSemaphoreStats") PartitionSemaphoreStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        PartitionSemaphoreStats deserialized = new PartitionSemaphoreStats(in);
        assertEquals(original, deserialized, "Writeable round-trip must produce equal object");
    }
}
