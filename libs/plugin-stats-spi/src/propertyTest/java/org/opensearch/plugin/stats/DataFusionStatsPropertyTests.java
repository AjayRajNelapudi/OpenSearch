/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link DataFusionStats} Writeable serialization and toXContent determinism.
 *
 * <p>Validates: Requirements 9.2, 9.3
 *
 * <p>Tag: Feature: plugin-stats-spi-lib, Property 1: DataFusionStats Writeable round-trip
 * <p>Tag: Feature: plugin-stats-spi-lib, Property 2: DataFusionStats toXContent determinism
 * <p>Tag: Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats
 */
public class DataFusionStatsPropertyTests {

    // ---- Arbitraries ----

    @Provide
    Arbitrary<NativeExecutorsStats.RuntimeMetrics> runtimeMetrics() {
        Arbitrary<Long> nonNeg = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(nonNeg, nonNeg, nonNeg, nonNeg, nonNeg, nonNeg, nonNeg, nonNeg)
            .as(NativeExecutorsStats.RuntimeMetrics::new);
    }

    @Provide
    Arbitrary<NativeExecutorsStats.TaskMonitorStats> taskMonitorStats() {
        Arbitrary<Long> nonNeg = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(nonNeg, nonNeg, nonNeg)
            .as(NativeExecutorsStats.TaskMonitorStats::new);
    }

    /** Builds a task monitor map with all 4 OperationType entries. */
    private Map<String, NativeExecutorsStats.TaskMonitorStats> buildMonitorMap(
        NativeExecutorsStats.TaskMonitorStats qe,
        NativeExecutorsStats.TaskMonitorStats sn,
        NativeExecutorsStats.TaskMonitorStats fp,
        NativeExecutorsStats.TaskMonitorStats ss
    ) {
        Map<String, NativeExecutorsStats.TaskMonitorStats> monitors = new LinkedHashMap<>();
        monitors.put(NativeExecutorsStats.OperationType.QUERY_EXECUTION.key(), qe);
        monitors.put(NativeExecutorsStats.OperationType.STREAM_NEXT.key(), sn);
        monitors.put(NativeExecutorsStats.OperationType.FETCH_PHASE.key(), fp);
        monitors.put(NativeExecutorsStats.OperationType.SEGMENT_STATS.key(), ss);
        return monitors;
    }

    /** DataFusionStats with both IO and CPU runtimes present. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsCpuPresent() {
        return Combinators.combine(
            runtimeMetrics(),
            runtimeMetrics(),
            taskMonitorStats(),
            taskMonitorStats(),
            taskMonitorStats(),
            taskMonitorStats()
        ).as((io, cpu, qe, sn, fp, ss) ->
            new DataFusionStats(new NativeExecutorsStats(io, cpu, buildMonitorMap(qe, sn, fp, ss)))
        );
    }

    /** DataFusionStats with IO runtime only (CPU absent). */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsCpuAbsent() {
        return Combinators.combine(
            runtimeMetrics(),
            taskMonitorStats(),
            taskMonitorStats(),
            taskMonitorStats(),
            taskMonitorStats()
        ).as((io, qe, sn, fp, ss) ->
            new DataFusionStats(new NativeExecutorsStats(io, null, buildMonitorMap(qe, sn, fp, ss)))
        );
    }

    /** DataFusionStats with null NativeExecutorsStats. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsNullExecutors() {
        return Arbitraries.just(new DataFusionStats((NativeExecutorsStats) null));
    }

    // ---- Arbitraries for ResourceUsageStats ----

    @Provide
    Arbitrary<ResourceUsageStats> resourceUsageStats() {
        return Arbitraries.doubles().between(-Double.MAX_VALUE, Double.MAX_VALUE)
            .edgeCases(config -> config.add(0.0, -0.0, 1.0, -1.0, Double.MIN_VALUE, Double.MAX_VALUE))
            .map(ResourceUsageStats::new);
    }

    /** NativeExecutorsStats with random IO runtime, optional CPU runtime, and 4 task monitors. */
    @Provide
    Arbitrary<NativeExecutorsStats> anyNativeExecutorsStats() {
        Arbitrary<Boolean> hasCpu = Arbitraries.of(true, false);
        return Combinators.combine(
            runtimeMetrics(),
            runtimeMetrics(),
            hasCpu,
            taskMonitorStats(),
            taskMonitorStats(),
            taskMonitorStats(),
            taskMonitorStats()
        ).as((io, cpu, cpuPresent, qe, sn, fp, ss) ->
            new NativeExecutorsStats(io, cpuPresent ? cpu : null, buildMonitorMap(qe, sn, fp, ss))
        );
    }

    /** DataFusionStats with random NativeExecutorsStats and non-null ResourceUsageStats. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsWithResourceUsage() {
        return Combinators.combine(anyNativeExecutorsStats(), resourceUsageStats())
            .as(DataFusionStats::new);
    }

    /** DataFusionStats with random NativeExecutorsStats and null ResourceUsageStats. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsWithNullResourceUsage() {
        return anyNativeExecutorsStats().map(nes -> new DataFusionStats(nes, null));
    }

    /** DataFusionStats with null NativeExecutorsStats and non-null ResourceUsageStats. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsNullExecutorsWithResourceUsage() {
        return resourceUsageStats().map(rus -> new DataFusionStats(null, rus));
    }

    /** DataFusionStats with optional NativeExecutorsStats and optional ResourceUsageStats. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsOptionalBoth() {
        Arbitrary<NativeExecutorsStats> optNes = anyNativeExecutorsStats().injectNull(0.3);
        Arbitrary<ResourceUsageStats> optRus = resourceUsageStats().injectNull(0.3);
        return Combinators.combine(optNes, optRus).as(DataFusionStats::new);
    }

    // ---- Property 1: DataFusionStats Writeable serialization round-trip ----

    /**
     * Feature: plugin-stats-spi-lib, Property 1: DataFusionStats Writeable round-trip (CPU present).
     *
     * <p>Validates: Requirements 9.2
     */
    @Property(tries = 100)
    @Tag("Feature: plugin-stats-spi-lib, Property 1: DataFusionStats Writeable round-trip")
    void writeableRoundTripCpuPresent(@ForAll("dataFusionStatsCpuPresent") DataFusionStats original) throws IOException {
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Writeable round-trip must preserve all fields (CPU present)");
    }

    /**
     * Feature: plugin-stats-spi-lib, Property 1: DataFusionStats Writeable round-trip (CPU absent).
     *
     * <p>Validates: Requirements 9.2
     */
    @Property(tries = 100)
    @Tag("Feature: plugin-stats-spi-lib, Property 1: DataFusionStats Writeable round-trip")
    void writeableRoundTripCpuAbsent(@ForAll("dataFusionStatsCpuAbsent") DataFusionStats original) throws IOException {
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Writeable round-trip must preserve all fields (CPU absent)");
    }

    /**
     * Feature: plugin-stats-spi-lib, Property 1: DataFusionStats Writeable round-trip (null executors).
     *
     * <p>Validates: Requirements 9.2
     */
    @Property(tries = 100)
    @Tag("Feature: plugin-stats-spi-lib, Property 1: DataFusionStats Writeable round-trip")
    void writeableRoundTripNullExecutors(@ForAll("dataFusionStatsNullExecutors") DataFusionStats original) throws IOException {
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Writeable round-trip must preserve null executors");
    }

    // ---- Helper ----

    private DataFusionStats writeableRoundTrip(DataFusionStats original) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(baos);
        original.writeTo(out);
        out.flush();
        byte[] bytes = baos.toByteArray();
        BytesStreamInput in = new BytesStreamInput(bytes);
        return new DataFusionStats(in);
    }

    // ---- Property 2: DataFusionStats toXContent determinism ----

    /**
     * Feature: plugin-stats-spi-lib, Property 2: DataFusionStats toXContent determinism (CPU present).
     *
     * <p>Validates: Requirements 9.3
     */
    @Property(tries = 100)
    @Tag("Feature: plugin-stats-spi-lib, Property 2: DataFusionStats toXContent determinism")
    void toXContentDeterminismCpuPresent(@ForAll("dataFusionStatsCpuPresent") DataFusionStats stats) throws IOException {
        byte[] first = renderJsonBytes(stats);
        byte[] second = renderJsonBytes(stats);
        assertTrue(Arrays.equals(first, second), "toXContent must produce byte-for-byte identical JSON on repeated calls (CPU present)");
    }

    /**
     * Feature: plugin-stats-spi-lib, Property 2: DataFusionStats toXContent determinism (CPU absent).
     *
     * <p>Validates: Requirements 9.3
     */
    @Property(tries = 100)
    @Tag("Feature: plugin-stats-spi-lib, Property 2: DataFusionStats toXContent determinism")
    void toXContentDeterminismCpuAbsent(@ForAll("dataFusionStatsCpuAbsent") DataFusionStats stats) throws IOException {
        byte[] first = renderJsonBytes(stats);
        byte[] second = renderJsonBytes(stats);
        assertTrue(Arrays.equals(first, second), "toXContent must produce byte-for-byte identical JSON on repeated calls (CPU absent)");
    }

    /**
     * Feature: plugin-stats-spi-lib, Property 2: DataFusionStats toXContent determinism (null executors).
     *
     * <p>Validates: Requirements 9.3
     */
    @Property(tries = 100)
    @Tag("Feature: plugin-stats-spi-lib, Property 2: DataFusionStats toXContent determinism")
    void toXContentDeterminismNullExecutors(@ForAll("dataFusionStatsNullExecutors") DataFusionStats stats) throws IOException {
        byte[] first = renderJsonBytes(stats);
        byte[] second = renderJsonBytes(stats);
        assertTrue(Arrays.equals(first, second), "toXContent must produce byte-for-byte identical JSON on repeated calls (null executors)");
    }

    /** Renders a {@link DataFusionStats} to JSON bytes via {@code toXContent}. */
    private byte[] renderJsonBytes(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        return BytesReference.toBytes(BytesReference.bytes(builder));
    }

    // ---- Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats ----

    /**
     * For any DataFusionStats with non-null ResourceUsageStats, serialization round-trip
     * preserves the ResourceUsageStats field.
     *
     * <p>Validates: Requirements 3.2, 3.3
     */
    @Property(tries = 100)
    @Tag("Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats")
    void roundTripPreservesResourceUsageStats(@ForAll("dataFusionStatsWithResourceUsage") DataFusionStats original) throws IOException {
        // Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Round-trip must preserve DataFusionStats with ResourceUsageStats");
        assertEquals(
            original.getResourceUsageStats(),
            deserialized.getResourceUsageStats(),
            "Round-trip must preserve ResourceUsageStats field"
        );
    }

    /**
     * For any DataFusionStats with null ResourceUsageStats, serialization round-trip
     * preserves the null.
     *
     * <p>Validates: Requirements 3.2, 3.3
     */
    @Property(tries = 100)
    @Tag("Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats")
    void roundTripPreservesNullResourceUsageStats(@ForAll("dataFusionStatsWithNullResourceUsage") DataFusionStats original) throws IOException {
        // Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Round-trip must preserve DataFusionStats with null ResourceUsageStats");
        assertEquals(null, deserialized.getResourceUsageStats(), "ResourceUsageStats must remain null after round-trip");
    }

    /**
     * For any DataFusionStats with null NativeExecutorsStats and non-null ResourceUsageStats,
     * serialization round-trip preserves both fields.
     *
     * <p>Validates: Requirements 3.2, 3.3
     */
    @Property(tries = 100)
    @Tag("Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats")
    void roundTripPreservesResourceUsageWithNullExecutors(@ForAll("dataFusionStatsNullExecutorsWithResourceUsage") DataFusionStats original) throws IOException {
        // Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Round-trip must preserve DataFusionStats with null executors and non-null ResourceUsageStats");
        assertEquals(
            original.getResourceUsageStats(),
            deserialized.getResourceUsageStats(),
            "ResourceUsageStats must be preserved when NativeExecutorsStats is null"
        );
    }

    /**
     * For any DataFusionStats with optional NativeExecutorsStats and optional ResourceUsageStats,
     * serialization round-trip preserves equality.
     *
     * <p>Validates: Requirements 3.2, 3.3
     */
    @Property(tries = 100)
    @Tag("Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats")
    void roundTripPreservesOptionalBothFields(@ForAll("dataFusionStatsOptionalBoth") DataFusionStats original) throws IOException {
        // Feature: native-memory-utilization, Property 2: DataFusionStats serialization round-trip preserves ResourceUsageStats
        DataFusionStats deserialized = writeableRoundTrip(original);
        assertEquals(original, deserialized, "Round-trip must preserve DataFusionStats with optional NativeExecutorsStats and ResourceUsageStats");
    }
}
