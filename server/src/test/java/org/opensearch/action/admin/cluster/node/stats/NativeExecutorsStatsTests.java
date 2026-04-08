/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link NativeExecutorsStats}.
 */
public class NativeExecutorsStatsTests {

    // --- @Provide methods for DataFusionPluginStats ---

    @Provide
    Arbitrary<DataFusionPluginStats.RuntimeValues> runtimeValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return posLong.array(long[].class).ofSize(22)
            .map(arr -> new DataFusionPluginStats.RuntimeValues(arr, 0));
    }

    @Provide
    Arbitrary<DataFusionPluginStats.TaskMonitorValues> taskMonitorValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return posLong.array(long[].class).ofSize(19)
            .map(arr -> new DataFusionPluginStats.TaskMonitorValues(arr, 0));
    }

    @Provide
    Arbitrary<DataFusionPluginStats> dataFusionPluginStats() {
        Arbitrary<DataFusionPluginStats.RuntimeValues> rv = runtimeValues();
        Arbitrary<DataFusionPluginStats.TaskMonitorValues> tm = taskMonitorValues();
        return Combinators.combine(
            rv.injectNull(0.3), rv.injectNull(0.3),
            tm, tm, tm, tm, tm
        ).as(DataFusionPluginStats::new);
    }

    // --- Property tests ---

    // Feature: remove-inflight-trackers, Property 1: NativeExecutorsStats serialization round-trip
    @Property(tries = 100)
    void writeableRoundTripProducesEqualObject(
            @ForAll("dataFusionPluginStats") DataFusionPluginStats pluginStats) throws IOException {
        NativeExecutorsStats original = new NativeExecutorsStats(pluginStats);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);
        assertEquals(original, deserialized);
    }

    // Feature: remove-inflight-trackers, Property 2: JSON output excludes in-flight and acquired fields
    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void xContentContainsExpectedStructure(
            @ForAll("dataFusionPluginStats") DataFusionPluginStats pluginStats) throws IOException {
        NativeExecutorsStats stats = new NativeExecutorsStats(pluginStats);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> json = XContentHelper.convertToMap(
            BytesReference.bytes(builder), true, builder.contentType()).v2();

        assertTrue(json.containsKey("task_monitors"));
        assertFalse(json.containsKey("native_inflight"),
            "native_inflight should be absent");
        assertFalse(json.toString().contains("in_flight"),
            "JSON output should not contain in_flight fields");
        assertFalse(json.toString().contains("acquired"),
            "JSON output should not contain acquired fields");
    }

    // Feature: rust-layer-rejection, Property 5: JSON output contains rejected field
    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void jsonOutputContainsRejectedFieldInEachTaskMonitor(
            @ForAll("dataFusionPluginStats") DataFusionPluginStats pluginStats) throws IOException {
        NativeExecutorsStats stats = new NativeExecutorsStats(pluginStats);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> json = XContentHelper.convertToMap(
            BytesReference.bytes(builder), true, builder.contentType()).v2();

        // Validates: Requirements 6.5, 7.1
        Map<String, Object> taskMonitors = (Map<String, Object>) json.get("task_monitors");
        String[] monitorNames = { "query_execution", "stream_next", "fetch_phase", "segment_stats", "indexed_query_execution" };
        for (String name : monitorNames) {
            Map<String, Object> monitor = (Map<String, Object>) taskMonitors.get(name);
            assertTrue(monitor.containsKey("rejected"),
                "task monitor '" + name + "' should contain a 'rejected' field");
        }
    }

    @Property(tries = 1)
    void constructorRejectsNullDataFusionPluginStats() {
        assertThrows(NullPointerException.class, () -> new NativeExecutorsStats((DataFusionPluginStats) null));
    }
}
