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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link NativeExecutorsStats}.
 */
public class NativeExecutorsStatsTests {

    private static final String[] OPERATION_NAMES = {
        "query_execution", "indexed_query_execution", "stream_next", "fetch_phase", "segment_stats"
    };

    // --- Existing @Provide methods for DataFusionPluginStats ---

    @Provide
    Arbitrary<DataFusionPluginStats.RuntimeValues> runtimeValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(posLong, posLong, posLong, posLong, posLong, posLong)
            .as((a, b, c, d, e, f) -> {
                long[] data = new long[] { a, b, c, d, e, f };
                return new DataFusionPluginStats.RuntimeValues(data, 0);
            });
    }

    @Provide
    Arbitrary<DataFusionPluginStats.TaskMonitorValues> taskMonitorValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(posLong, posLong, posLong)
            .as((a, b, c) -> {
                long[] data = new long[] { a, b, c };
                return new DataFusionPluginStats.TaskMonitorValues(data, 0);
            });
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

    // --- Per-operation map providers ---

    @Provide
    Arbitrary<Map<String, long[]>> perOperationMap() {
        Arbitrary<Long> inFlight = Arbitraries.longs().between(0, 10000);
        Arbitrary<Long> acquired = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        // Generate a subset of operation names (1 to 5 entries)
        return Arbitraries.integers().between(1, OPERATION_NAMES.length).flatMap(count ->
            Combinators.combine(
                Arbitraries.of(count),
                inFlight.list().ofSize(count),
                acquired.list().ofSize(count)
            ).as((c, inFlights, acquireds) -> {
                Map<String, long[]> map = new LinkedHashMap<>(c);
                for (int i = 0; i < c; i++) {
                    map.put(OPERATION_NAMES[i], new long[] { inFlights.get(i), acquireds.get(i) });
                }
                return map;
            })
        );
    }

    @Provide
    Arbitrary<NativeExecutorsStats> nativeExecutorsStatsWithPerOp() {
        return Combinators.combine(
            dataFusionPluginStats(),
            perOperationMap()
        ).as(NativeExecutorsStats::new);
    }

    @Provide
    Arbitrary<NativeExecutorsStats> nativeExecutorsStatsWithoutPerOp() {
        return dataFusionPluginStats().map(NativeExecutorsStats::new);
    }

    @Provide
    Arbitrary<Long> rejectionCount() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
    }

    @Provide
    Arbitrary<NativeExecutorsStats> nativeExecutorsStatsWithRejections() {
        return Combinators.combine(
            dataFusionPluginStats(),
            perOperationMap(),
            rejectionCount()
        ).as(NativeExecutorsStats::new);
    }

    // --- Property tests ---

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
            "native_inflight should be absent when no per-operation data");
    }

    @Property(tries = 1)
    void constructorRejectsNullDataFusionPluginStats() {
        assertThrows(NullPointerException.class, () -> new NativeExecutorsStats((DataFusionPluginStats) null));
    }

    // Feature: per-operation-native-tracker, Property 6: Stats serialization round-trip
    // **Validates: Requirements 6.1, 8.1**

    @Property(tries = 100)
    void statsSerializationRoundTripWithPerOpValues(
            @ForAll("nativeExecutorsStatsWithPerOp") NativeExecutorsStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

        assertEquals(original, deserialized);
        assertEquals(original.getPerOperationInflight().size(), deserialized.getPerOperationInflight().size());
    }

    @Property(tries = 100)
    void statsSerializationRoundTripWithoutPerOpValues(
            @ForAll("nativeExecutorsStatsWithoutPerOp") NativeExecutorsStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

        assertEquals(original, deserialized);
        assertTrue(deserialized.getPerOperationInflight().isEmpty());
    }

    // Feature: per-operation-native-tracker, Property 7: JSON per-operation breakdown with correct totals
    // **Validates: Requirements 6.2, 6.3, 6.4, 6.5**

    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void jsonPerOperationBreakdownWithCorrectTotals(
            @ForAll("nativeExecutorsStatsWithPerOp") NativeExecutorsStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> json = XContentHelper.convertToMap(
            BytesReference.bytes(builder), true, builder.contentType()).v2();

        Map<String, long[]> perOp = stats.getPerOperationInflight();

        assertFalse(json.containsKey("native_inflight"),
            "native_inflight should no longer be a separate section");
        assertTrue(json.containsKey("task_monitors"),
            "task_monitors should be present");
        Map<String, Object> taskMonitors = (Map<String, Object>) json.get("task_monitors");
        assertNotNull(taskMonitors);

        long expectedTotalInFlight = 0;
        long expectedTotalAcquired = 0;

        for (Map.Entry<String, long[]> entry : perOp.entrySet()) {
            String opName = entry.getKey();
            long[] values = entry.getValue();
            assertTrue(taskMonitors.containsKey(opName),
                "task_monitors should contain operation: " + opName);
            Map<String, Object> opObj = (Map<String, Object>) taskMonitors.get(opName);
            assertEquals(values[0], ((Number) opObj.get("in_flight")).longValue(),
                "in_flight mismatch for " + opName);
            assertEquals(values[1], ((Number) opObj.get("acquired")).longValue(),
                "acquired mismatch for " + opName);
            expectedTotalInFlight += values[0];
            expectedTotalAcquired += values[1];
        }

        // total_in_flight and total_acquired are now in the tasks section, not task_monitors
        assertFalse(taskMonitors.containsKey("total_in_flight"),
            "total_in_flight should not be inside task_monitors");
        assertFalse(taskMonitors.containsKey("total_acquired"),
            "total_acquired should not be inside task_monitors");

        assertTrue(json.containsKey("tasks"), "tasks section should be present when perOp is non-empty");
        Map<String, Object> tasks = (Map<String, Object>) json.get("tasks");
        assertEquals(expectedTotalInFlight, ((Number) tasks.get("total_in_flight")).longValue(),
            "total_in_flight should equal sum of per-operation in_flight values");
        assertEquals(expectedTotalAcquired, ((Number) tasks.get("total_acquired")).longValue(),
            "total_acquired should equal sum of per-operation acquired values");
    }

    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void jsonOmitsNativeInflightWhenMapEmpty(
            @ForAll("nativeExecutorsStatsWithoutPerOp") NativeExecutorsStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> json = XContentHelper.convertToMap(
            BytesReference.bytes(builder), true, builder.contentType()).v2();

        assertFalse(json.containsKey("native_inflight"),
            "native_inflight should be absent when per-operation map is empty");
        assertTrue(json.containsKey("task_monitors"));
        // tasks section always present with zeros when no trackers registered
        assertTrue(json.containsKey("tasks"), "tasks section should always be present");
        Map<String, Object> tasks = (Map<String, Object>) json.get("tasks");
        assertEquals(0L, ((Number) tasks.get("total_in_flight")).longValue());
        assertEquals(0L, ((Number) tasks.get("total_acquired")).longValue());
        assertEquals(0L, ((Number) tasks.get("rejections")).longValue());
    }

    // ===================================================================
    // Feature: tokio-metrics-rejection, Property 6: Stats serialization round-trip
    // **Validates: Requirements 4.3**
    // ===================================================================

    @Property(tries = 100)
    void statsSerializationRoundTripWithRejections(
            @ForAll("nativeExecutorsStatsWithRejections") NativeExecutorsStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

        assertEquals(original, deserialized);
        assertEquals(original.getRejections(), deserialized.getRejections(),
            "rejections counter should survive serialization round-trip");
        assertEquals(original.getPerOperationInflight().size(), deserialized.getPerOperationInflight().size());
    }

    // ===================================================================
    // Feature: tokio-metrics-rejection, Property 7: Stats XContent contains required fields
    // **Validates: Requirements 4.2, 4.4**
    // ===================================================================

    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void xContentContainsTasksSectionWithRequiredFields(
            @ForAll("nativeExecutorsStatsWithRejections") NativeExecutorsStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        Map<String, Object> json = XContentHelper.convertToMap(
            BytesReference.bytes(builder), true, builder.contentType()).v2();

        // tasks section should be present when perOperationInflight is non-empty
        assertTrue(json.containsKey("tasks"),
            "tasks section should be present when plugin is installed");
        Map<String, Object> tasks = (Map<String, Object>) json.get("tasks");
        assertNotNull(tasks);
        assertTrue(tasks.containsKey("total_in_flight"),
            "tasks section should contain total_in_flight");
        assertTrue(tasks.containsKey("total_acquired"),
            "tasks section should contain total_acquired");
        assertTrue(tasks.containsKey("rejections"),
            "tasks section should contain rejections");
        assertEquals(stats.getRejections(), ((Number) tasks.get("rejections")).longValue(),
            "rejections value should match the stats object");
    }

    // ===================================================================
    // Unit tests for NativeExecutorsStats tasks section (Task 7.4)
    // **Validates: Requirements 4.2, 4.3, 4.4, 4.5**
    // ===================================================================

    @Property(tries = 1)
    void serializationRoundTripWithRejectionCounter() throws IOException {
        DataFusionPluginStats pluginStats = createMinimalPluginStats();
        Map<String, long[]> perOp = new LinkedHashMap<>();
        perOp.put("query_execution", new long[] { 5, 100 });
        perOp.put("stream_next", new long[] { 3, 200 });
        long rejections = 42;

        NativeExecutorsStats original = new NativeExecutorsStats(pluginStats, perOp, rejections);
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

        assertEquals(original, deserialized);
        assertEquals(42, deserialized.getRejections());
    }

    @Property(tries = 1)
    @SuppressWarnings("unchecked")
    void xContentIncludesTasksSectionWithAllFields() throws IOException {
        DataFusionPluginStats pluginStats = createMinimalPluginStats();
        Map<String, long[]> perOp = new LinkedHashMap<>();
        perOp.put("query_execution", new long[] { 5, 100 });
        perOp.put("stream_next", new long[] { 3, 200 });
        long rejections = 42;

        NativeExecutorsStats stats = new NativeExecutorsStats(pluginStats, perOp, rejections);
        Map<String, Object> json = toJsonMap(stats);

        assertTrue(json.containsKey("tasks"), "tasks section should be present");
        Map<String, Object> tasks = (Map<String, Object>) json.get("tasks");
        assertEquals(8L, ((Number) tasks.get("total_in_flight")).longValue());
        assertEquals(300L, ((Number) tasks.get("total_acquired")).longValue());
        assertEquals(42L, ((Number) tasks.get("rejections")).longValue());
    }

    @Property(tries = 1)
    @SuppressWarnings("unchecked")
    void totalInFlightAndTotalAcquiredNotInsideTaskMonitors() throws IOException {
        DataFusionPluginStats pluginStats = createMinimalPluginStats();
        Map<String, long[]> perOp = new LinkedHashMap<>();
        perOp.put("query_execution", new long[] { 5, 100 });

        NativeExecutorsStats stats = new NativeExecutorsStats(pluginStats, perOp, 10);
        Map<String, Object> json = toJsonMap(stats);

        Map<String, Object> taskMonitors = (Map<String, Object>) json.get("task_monitors");
        assertNotNull(taskMonitors);
        assertFalse(taskMonitors.containsKey("total_in_flight"),
            "total_in_flight should not be inside task_monitors");
        assertFalse(taskMonitors.containsKey("total_acquired"),
            "total_acquired should not be inside task_monitors");
    }

    @Property(tries = 1)
    @SuppressWarnings("unchecked")
    void pluginInstalledButNoTrackersShowsTasksWithZeros() throws IOException {
        DataFusionPluginStats pluginStats = createMinimalPluginStats();
        // Empty perOperationInflight = no trackers registered yet, but plugin is installed
        NativeExecutorsStats stats = new NativeExecutorsStats(pluginStats);
        Map<String, Object> json = toJsonMap(stats);

        assertTrue(json.containsKey("tasks"),
            "tasks section should always be present when NativeExecutorsStats is constructed");
        Map<String, Object> tasks = (Map<String, Object>) json.get("tasks");
        assertEquals(0L, ((Number) tasks.get("total_in_flight")).longValue());
        assertEquals(0L, ((Number) tasks.get("total_acquired")).longValue());
        assertEquals(0L, ((Number) tasks.get("rejections")).longValue());
    }

    // --- Helpers ---

    private DataFusionPluginStats createMinimalPluginStats() {
        long[] tmData = new long[] { 0, 0, 0 };
        DataFusionPluginStats.TaskMonitorValues tm = new DataFusionPluginStats.TaskMonitorValues(tmData, 0);
        return new DataFusionPluginStats(null, null, tm, tm, tm, tm, tm);
    }

    private Map<String, Object> toJsonMap(NativeExecutorsStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return XContentHelper.convertToMap(
            BytesReference.bytes(builder), true, builder.contentType()).v2();
    }
}
