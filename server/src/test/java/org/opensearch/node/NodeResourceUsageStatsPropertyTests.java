/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Locale;

/**
 * Property-based tests for {@link NodeResourceUsageStats} serialization round-trip.
 *
 * <p>Validates: Requirements 5.2, 5.3
 *
 * <p>Since jqwik is not available in the server module, this uses OpenSearchTestCase
 * with randomDouble() in a loop of 100 iterations.
 */
public class NodeResourceUsageStatsPropertyTests extends OpenSearchTestCase {

    // Feature: native-memory-utilization, Property 3: NodeResourceUsageStats serialization round-trip preserves nativeMemoryUtilization

    /**
     * For any valid NodeResourceUsageStats with random field values including nativeMemoryUtilization,
     * serializing via writeTo and deserializing via the StreamInput constructor produces a
     * NodeResourceUsageStats with an equal nativeMemoryUtilization value.
     *
     * <p>Validates: Requirements 5.2, 5.3
     */
    public void testSerializationRoundTripPreservesNativeMemoryUtilization() throws IOException {
        for (int i = 0; i < 100; i++) {
            String nodeId = randomAlphaOfLengthBetween(3, 10);
            long timestamp = randomNonNegativeLong();
            double memoryUtilizationPercent = randomDouble();
            double cpuUtilizationPercent = randomDouble();
            IoUsageStats ioUsageStats = randomBoolean() ? new IoUsageStats(randomDouble()) : null;
            double nativeMemoryUtilization = randomDouble();

            NodeResourceUsageStats original = new NodeResourceUsageStats(
                nodeId,
                timestamp,
                memoryUtilizationPercent,
                cpuUtilizationPercent,
                ioUsageStats,
                nativeMemoryUtilization
            );

            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(Version.V_3_7_0);
                original.writeTo(out);

                StreamInput in = out.bytes().streamInput();
                in.setVersion(Version.V_3_7_0);
                NodeResourceUsageStats deserialized = new NodeResourceUsageStats(in);

                assertEquals(
                    "Iteration " + i + ": nativeMemoryUtilization must be preserved after round-trip",
                    original.getNativeMemoryUtilization(),
                    deserialized.getNativeMemoryUtilization(),
                    0.0
                );
                assertEquals(
                    "Iteration " + i + ": nodeId must be preserved",
                    nodeId,
                    deserialized.nodeId
                );
                assertEquals(
                    "Iteration " + i + ": timestamp must be preserved",
                    original.getTimestamp(),
                    deserialized.getTimestamp()
                );
                assertEquals(
                    "Iteration " + i + ": cpuUtilizationPercent must be preserved",
                    original.getCpuUtilizationPercent(),
                    deserialized.getCpuUtilizationPercent(),
                    0.0
                );
                assertEquals(
                    "Iteration " + i + ": memoryUtilizationPercent must be preserved",
                    original.getMemoryUtilizationPercent(),
                    deserialized.getMemoryUtilizationPercent(),
                    0.0
                );
            }
        }
    }

    // Feature: native-memory-utilization, Property 4: NodeResourceUsageStats XContent formats nativeMemoryUtilization correctly

    /**
     * For any valid double value used as nativeMemoryUtilization, the XContent output of
     * NodeResourceUsageStats SHALL contain a "native_memory_utilization" field whose string
     * value equals {@code String.format(Locale.ROOT, "%.1f", value)}.
     *
     * <p><b>Validates: Requirements 5.4</b>
     */
    public void testXContentFormatsNativeMemoryUtilizationCorrectly() throws IOException {
        for (int i = 0; i < 100; i++) {
            String nodeId = randomAlphaOfLengthBetween(3, 10);
            long timestamp = randomNonNegativeLong();
            double memoryUtilizationPercent = randomDouble();
            double cpuUtilizationPercent = randomDouble();
            IoUsageStats ioUsageStats = randomBoolean() ? new IoUsageStats(randomDouble()) : null;
            double nativeMemoryUtilization = randomDouble();

            NodeResourceUsageStats stats = new NodeResourceUsageStats(
                nodeId,
                timestamp,
                memoryUtilizationPercent,
                cpuUtilizationPercent,
                ioUsageStats,
                nativeMemoryUtilization
            );

            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String json = builder.toString();

            String expectedFormatted = String.format(Locale.ROOT, "%.1f", nativeMemoryUtilization);
            String expectedFragment = "\"native_memory_utilization\":\"" + expectedFormatted + "\"";

            assertTrue(
                "Iteration " + i + ": XContent output must contain native_memory_utilization formatted as '"
                    + expectedFormatted + "', but got: " + json,
                json.contains(expectedFragment)
            );
        }
    }
}
