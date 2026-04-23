/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Property-based tests for {@link ResourceUsageStats} serialization round-trip.
 *
 * <p>Validates: Requirements 2.2, 2.5
 *
 * <p>Tag: Feature: native-memory-utilization, Property 1: ResourceUsageStats serialization round-trip
 */
public class ResourceUsageStatsPropertyTests {

    // Feature: native-memory-utilization, Property 1: ResourceUsageStats serialization round-trip

    @Provide
    Arbitrary<Double> anyDouble() {
        return Arbitraries.doubles().between(-Double.MAX_VALUE, Double.MAX_VALUE).edgeCases(config -> {
            config.add(0.0, -0.0, 1.0, -1.0, Double.MIN_VALUE, Double.MAX_VALUE);
        });
    }

    /**
     * For any valid double value, constructing a ResourceUsageStats, serializing via writeTo,
     * and deserializing via the StreamInput constructor produces a ResourceUsageStats with an
     * equal nativeMemoryUtilization value.
     *
     * <p>Validates: Requirements 2.2, 2.5
     */
    @Property(tries = 100)
    @Tag("Feature: native-memory-utilization, Property 1: ResourceUsageStats serialization round-trip")
    void serializationRoundTripPreservesNativeMemoryUtilization(@ForAll("anyDouble") double value) throws IOException {
        ResourceUsageStats original = new ResourceUsageStats(value);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(baos);
        original.writeTo(out);
        out.flush();

        byte[] bytes = baos.toByteArray();
        BytesStreamInput in = new BytesStreamInput(bytes);
        ResourceUsageStats deserialized = new ResourceUsageStats(in);

        assertEquals(
            original.getNativeMemoryUtilization(),
            deserialized.getNativeMemoryUtilization(),
            "Writeable round-trip must preserve nativeMemoryUtilization"
        );
        assertEquals(original, deserialized, "Writeable round-trip must produce equal ResourceUsageStats");
    }
}
