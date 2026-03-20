/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.vectorized.execution.metrics.PluginStats;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Property-based tests for NodeStats serialization round-trip with native metrics.
 *
 * NodeStats holds a single {@code @Nullable PluginStats dataFusionPluginStats} field
 * serialized via {@code writeOptionalNamedWriteable} / {@code readOptionalNamedWriteable}.
 * Since the concrete DataFusionPluginStats lives in the plugin jar (not available to
 * server tests), this test uses a minimal test-only PluginStats implementation to
 * verify the NamedWriteable round-trip mechanism through NodeStats.
 *
 * Validates: Requirements 7.7, 11.4
 */
public class NodeStatsNativeMetricRoundTripTests {

    /**
     * Minimal PluginStats implementation for testing the NamedWriteable round-trip
     * through NodeStats. Exercises the same code path as DataFusionPluginStats
     * (writeOptionalNamedWriteable / readOptionalNamedWriteable) without requiring
     * a dependency on the plugin module.
     */
    static class TestPluginStats implements PluginStats {

        static final String NAME = "test_plugin";

        private final long metricA;
        private final long metricB;
        private final double ratio;

        TestPluginStats(long metricA, long metricB, double ratio) {
            this.metricA = metricA;
            this.metricB = metricB;
            this.ratio = ratio;
        }

        TestPluginStats(StreamInput in) throws IOException {
            this.metricA = in.readVLong();
            this.metricB = in.readVLong();
            this.ratio = in.readDouble();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(metricA);
            out.writeVLong(metricB);
            out.writeDouble(ratio);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("metric_a", metricA);
            builder.field("metric_b", metricB);
            builder.field("ratio", ratio);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestPluginStats that = (TestPluginStats) o;
            return metricA == that.metricA && metricB == that.metricB
                && Double.compare(that.ratio, ratio) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(metricA, metricB, ratio);
        }
    }

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(List.of(
        new NamedWriteableRegistry.Entry(PluginStats.class, TestPluginStats.NAME, TestPluginStats::new)
    ));

    // --- Arbitraries ---

    @Provide
    Arbitrary<TestPluginStats> testPluginStats() {
        return Combinators.combine(
            Arbitraries.longs().between(0, Long.MAX_VALUE / 2),
            Arbitraries.longs().between(0, Long.MAX_VALUE / 2),
            Arbitraries.doubles().between(0.0, 1.0)
        ).as(TestPluginStats::new);
    }

    // --- Property: null PluginStats round-trip ---

    /**
     * Property: NodeStats Writeable round-trip with null PluginStats
     *
     * For any NodeStats with null dataFusionPluginStats, serializing via
     * writeTo(StreamOutput) and deserializing via new NodeStats(StreamInput)
     * SHALL produce a NodeStats with null getDataFusionPluginStats().
     *
     * Validates: Requirements 7.1
     */
    @Property(tries = 10)
    void writeableRoundTripPreservesNullPluginStats() throws IOException {
        NodeStats original = createNodeStats(null);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NodeStats deserialized = new NodeStats(in);

        assertNull(deserialized.getDataFusionPluginStats(),
            "dataFusionPluginStats should be null after round-trip when not set");
    }

    // --- Property 4: NodeStats NamedWriteable round-trip ---

    /**
     * Property 4: NodeStats NamedWriteable round-trip
     *
     * For any valid NodeStats with a non-null PluginStats, serializing and
     * deserializing via StreamOutput/StreamInput with NamedWriteableRegistry
     * containing the plugin entry SHALL produce a NodeStats with an equal
     * PluginStats field.
     *
     * This exercises the same writeOptionalNamedWriteable / readOptionalNamedWriteable
     * code path that DataFusionPluginStats uses in production.
     *
     * **Validates: Requirements 11.4, 7.7**
     */
    @Property(tries = 100)
    void namedWriteableRoundTripPreservesPluginStats(
            @ForAll("testPluginStats") TestPluginStats pluginStats) throws IOException {
        NodeStats original = createNodeStats(pluginStats);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize with NamedWriteableRegistry so readOptionalNamedWriteable resolves
        StreamInput rawIn = out.bytes().streamInput();
        StreamInput in = new NamedWriteableAwareStreamInput(rawIn, REGISTRY);
        NodeStats deserialized = new NodeStats(in);

        // Verify the PluginStats field survived the round-trip
        assertNotNull(deserialized.getDataFusionPluginStats(),
            "dataFusionPluginStats should be non-null after round-trip");
        assertEquals(pluginStats, deserialized.getDataFusionPluginStats(),
            "PluginStats should be equal after NamedWriteable round-trip through NodeStats");
    }

    // --- Helper ---

    private NodeStats createNodeStats(PluginStats pluginStats) {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            new TransportAddress(TransportAddress.META_ADDRESS, 9200),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        return new NodeStats(
            node,
            System.currentTimeMillis(),
            null, // indices
            null, // os
            null, // process
            null, // jvm
            null, // threadPool
            null, // fs
            null, // transport
            null, // http
            null, // breaker
            null, // scriptStats
            null, // discoveryStats
            null, // ingestStats
            null, // adaptiveSelectionStats
            null, // resourceUsageStats
            null, // scriptCacheStats
            null, // indexingPressureStats
            null, // shardIndexingPressureStats
            null, // searchBackpressureStats
            null, // clusterManagerThrottlingStats
            null, // weightedRoutingStats
            null, // fileCacheStats
            null, // taskCancellationStats
            null, // searchPipelineStats
            null, // segmentReplicationRejectionStats
            null, // repositoriesStats
            null, // admissionControlStats
            null, // nodeCacheStats
            null, // remoteStoreNodeStats
            pluginStats  // dataFusionPluginStats
        );
    }
}
