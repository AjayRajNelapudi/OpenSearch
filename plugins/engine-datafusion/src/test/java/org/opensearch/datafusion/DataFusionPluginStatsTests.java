/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.datafusion.proto.DataFusionStatsProto;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for DataFusionPluginStats using jqwik.
 * Tests protobuf decode round-trip, Writeable round-trip, and XContent structure.
 *
 * Feature: pluginstats-migration
 */
public class DataFusionPluginStatsTests {

    // --- Arbitraries ---

    @Provide
    Arbitrary<long[]> longArray28() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2)
            .array(long[].class).ofSize(28);
    }

    @Provide
    Arbitrary<DataFusionPluginStats.RuntimeValues> runtimeValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        // Split 28 fields across multiple combine calls (jqwik max 8 per combine)
        return Combinators.combine(
            posLong, posLong, posLong, posLong, posLong, posLong, posLong, posLong
        ).as((a, b, c, d, e, f, g, h) -> new long[]{a, b, c, d, e, f, g, h})
        .flatMap(first8 -> Combinators.combine(
            posLong, posLong, posLong, posLong, posLong, posLong, posLong, posLong
        ).as((a, b, c, d, e, f, g, h) -> new long[]{a, b, c, d, e, f, g, h})
        .flatMap(second8 -> Combinators.combine(
            posLong, posLong, posLong, posLong, posLong, posLong, posLong, posLong
        ).as((a, b, c, d, e, f, g, h) -> new long[]{a, b, c, d, e, f, g, h})
        .flatMap(third8 -> Combinators.combine(
            posLong, posLong, posLong, posLong
        ).as((a, b, c, d) -> new DataFusionPluginStats.RuntimeValues(
            first8[0], first8[1], first8[2], first8[3],
            first8[4], first8[5], first8[6],
            first8[7], second8[0], second8[1], second8[2],
            second8[3], second8[4], second8[5],
            second8[6], second8[7], third8[0],
            third8[1], third8[2], third8[3],
            third8[4], third8[5], third8[6],
            third8[7], a, b, c, d
        )))));
    }

    @Provide
    Arbitrary<DataFusionPluginStats.TaskMonitorValues> taskMonitorValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        Arbitrary<Double> ratio = Arbitraries.doubles().between(0.0, 1.0);
        return Combinators.combine(posLong, posLong, posLong, posLong, posLong, ratio)
            .as(DataFusionPluginStats.TaskMonitorValues::new);
    }

    @Provide
    Arbitrary<DataFusionPluginStats> pluginStats() {
        Arbitrary<DataFusionPluginStats.RuntimeValues> rv = runtimeValues();
        Arbitrary<DataFusionPluginStats.TaskMonitorValues> tm = taskMonitorValues();
        return Combinators.combine(
            rv,                    // ioRuntime (always non-null)
            rv.injectNull(0.3),    // cpuRuntime (30% chance null)
            tm, tm, tm, tm         // 4 task monitors (always non-null)
        ).as(DataFusionPluginStats::new);
    }

    @Provide
    Arbitrary<DataFusionPluginStats> pluginStatsWithNullCpu() {
        Arbitrary<DataFusionPluginStats.RuntimeValues> rv = runtimeValues();
        Arbitrary<DataFusionPluginStats.TaskMonitorValues> tm = taskMonitorValues();
        return Combinators.combine(rv, tm, tm, tm, tm)
            .as((io, qe, sn, fp, ss) -> new DataFusionPluginStats(io, null, qe, sn, fp, ss));
    }

    // --- Property 1: Protobuf decode round-trip ---

    /**
     * Property 1: DataFusionPluginStats protobuf decode round-trip
     *
     * For any valid set of runtime + task monitor metric values, encoding to protobuf
     * and decoding via DataFusionPluginStats.decode(byte[]) SHALL produce an object
     * with field values matching the originals.
     *
     * **Validates: Requirements 4.4**
     */
    @Property(tries = 100)
    void protobufDecodeRoundTrip(@ForAll("longArray28") long[] ioFields,
                                  @ForAll("longArray28") long[] cpuFields,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues qe,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues sn,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues fp,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues ss) {
        // Build protobuf message
        DataFusionStatsProto.RuntimeMetrics ioProto = buildRuntimeMetricsProto(ioFields);
        DataFusionStatsProto.RuntimeMetrics cpuProto = buildRuntimeMetricsProto(cpuFields);

        DataFusionStatsProto.DataFusionStats proto = DataFusionStatsProto.DataFusionStats.newBuilder()
            .setIoRuntime(ioProto)
            .setCpuRuntime(cpuProto)
            .setTaskMonitors(DataFusionStatsProto.TaskMonitors.newBuilder()
                .setQueryExecution(buildTaskMonitorProto(qe))
                .setStreamNext(buildTaskMonitorProto(sn))
                .setFetchPhase(buildTaskMonitorProto(fp))
                .setSegmentStats(buildTaskMonitorProto(ss))
                .build())
            .build();

        byte[] bytes = proto.toByteArray();
        DataFusionPluginStats decoded = DataFusionPluginStats.decode(bytes);

        // Verify IO runtime fields
        assertNotNull(decoded.getIoRuntime());
        assertRuntimeValuesMatch(decoded.getIoRuntime(), ioFields);

        // Verify CPU runtime fields
        assertNotNull(decoded.getCpuRuntime());
        assertRuntimeValuesMatch(decoded.getCpuRuntime(), cpuFields);

        // Verify task monitor fields
        assertTaskMonitorEquals(qe, decoded.getQueryExecution());
        assertTaskMonitorEquals(sn, decoded.getStreamNext());
        assertTaskMonitorEquals(fp, decoded.getFetchPhase());
        assertTaskMonitorEquals(ss, decoded.getSegmentStats());
    }

    // --- Property 2: Writeable round-trip ---

    /**
     * Property 2: DataFusionPluginStats Writeable round-trip
     *
     * For any valid DataFusionPluginStats object, serializing via writeTo(StreamOutput)
     * and deserializing via new DataFusionPluginStats(StreamInput) SHALL produce an
     * object equal to the original.
     *
     * **Validates: Requirements 4.10**
     */
    @Property(tries = 100)
    void writeableRoundTrip(@ForAll("pluginStats") DataFusionPluginStats original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        DataFusionPluginStats deserialized = new DataFusionPluginStats(in);

        assertEquals(original, deserialized, "Writeable round-trip should produce equal objects");
    }

    // --- Property 3: XContent structure ---

    /**
     * Property 3: DataFusionPluginStats XContent structure
     *
     * For any valid DataFusionPluginStats object, toXContent() SHALL produce JSON
     * containing io_runtime, task_monitors keys, and optionally cpu_runtime,
     * with field values matching the object's fields.
     *
     * **Validates: Requirements 4.8, 4.11**
     */
    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void xcontentStructure(@ForAll("pluginStats") DataFusionPluginStats original) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        // Parse JSON into a map
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(null, null, builder.toString());
        Map<String, Object> map = parser.map();

        // task_monitors key must always be present
        assertTrue(map.containsKey("task_monitors"), "JSON must contain task_monitors key");
        Map<String, Object> taskMonitors = (Map<String, Object>) map.get("task_monitors");
        assertNotNull(taskMonitors);
        assertTrue(taskMonitors.containsKey("query_execution"));
        assertTrue(taskMonitors.containsKey("stream_next"));
        assertTrue(taskMonitors.containsKey("fetch_phase"));
        assertTrue(taskMonitors.containsKey("segment_stats"));

        // io_runtime present when non-null
        if (original.getIoRuntime() != null) {
            assertTrue(map.containsKey("io_runtime"), "JSON must contain io_runtime when ioRuntime is non-null");
            Map<String, Object> ioMap = (Map<String, Object>) map.get("io_runtime");
            assertEquals(original.getIoRuntime().getWorkersCount(), ((Number) ioMap.get("workers_count")).longValue());
            assertEquals(original.getIoRuntime().getGlobalQueueDepth(), ((Number) ioMap.get("global_queue_depth")).longValue());
        }

        // cpu_runtime present only when non-null
        if (original.getCpuRuntime() != null) {
            assertTrue(map.containsKey("cpu_runtime"), "JSON must contain cpu_runtime when cpuRuntime is non-null");
            Map<String, Object> cpuMap = (Map<String, Object>) map.get("cpu_runtime");
            assertEquals(original.getCpuRuntime().getWorkersCount(), ((Number) cpuMap.get("workers_count")).longValue());
        } else {
            assertTrue(!map.containsKey("cpu_runtime"), "JSON must not contain cpu_runtime when cpuRuntime is null");
        }

        // Verify task monitor values match
        Map<String, Object> qeMap = (Map<String, Object>) taskMonitors.get("query_execution");
        assertEquals(original.getQueryExecution().getTotalPollDurationMs(),
            ((Number) qeMap.get("total_poll_duration_ms")).longValue());
        assertEquals(original.getQueryExecution().getSlowPollRatio(),
            ((Number) qeMap.get("slow_poll_ratio")).doubleValue(), 1e-10);
    }

    // --- Edge case: absent CPU runtime → null cpuRuntime ---

    /**
     * Edge case: When protobuf message has no cpu_runtime field set,
     * decoded DataFusionPluginStats should have null cpuRuntime.
     *
     * **Validates: Requirements 4.4**
     */
    @Property(tries = 50)
    void absentCpuRuntimeDecodesToNull(@ForAll("longArray28") long[] ioFields,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues qe,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues sn,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues fp,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues ss) {
        // Build protobuf WITHOUT cpu_runtime
        DataFusionStatsProto.DataFusionStats proto = DataFusionStatsProto.DataFusionStats.newBuilder()
            .setIoRuntime(buildRuntimeMetricsProto(ioFields))
            .setTaskMonitors(DataFusionStatsProto.TaskMonitors.newBuilder()
                .setQueryExecution(buildTaskMonitorProto(qe))
                .setStreamNext(buildTaskMonitorProto(sn))
                .setFetchPhase(buildTaskMonitorProto(fp))
                .setSegmentStats(buildTaskMonitorProto(ss))
                .build())
            .build();

        DataFusionPluginStats decoded = DataFusionPluginStats.decode(proto.toByteArray());
        assertNull(decoded.getCpuRuntime(), "cpuRuntime should be null when absent from protobuf");
        assertNotNull(decoded.getIoRuntime(), "ioRuntime should still be present");
    }

    // --- Edge case: invalid bytes → IllegalArgumentException ---

    /**
     * Edge case: Passing invalid (non-protobuf) bytes to decode() should throw
     * IllegalArgumentException.
     *
     * **Validates: Requirements 4.4**
     */
    @Property(tries = 1)
    void invalidBytesThrowsIllegalArgumentException() {
        // A varint that signals "more bytes follow" (high bit set) but is truncated.
        // Field 1, wire type 0 (varint), then 0x80 means "continuation" with no following byte.
        byte[] truncatedVarint = new byte[]{0x08, (byte) 0x80};
        assertThrows(IllegalArgumentException.class, () -> DataFusionPluginStats.decode(truncatedVarint),
            "decode() should throw IllegalArgumentException for invalid protobuf bytes");
    }

    // --- Helper methods ---

    private DataFusionStatsProto.RuntimeMetrics buildRuntimeMetricsProto(long[] fields) {
        return DataFusionStatsProto.RuntimeMetrics.newBuilder()
            .setWorkersCount(fields[0])
            .setTotalParkCount(fields[1])
            .setMaxParkCount(fields[2])
            .setMinParkCount(fields[3])
            .setTotalNoopCount(fields[4])
            .setMaxNoopCount(fields[5])
            .setMinNoopCount(fields[6])
            .setTotalStealCount(fields[7])
            .setMaxStealCount(fields[8])
            .setMinStealCount(fields[9])
            .setTotalStealOperations(fields[10])
            .setTotalLocalScheduleCount(fields[11])
            .setMaxLocalScheduleCount(fields[12])
            .setMinLocalScheduleCount(fields[13])
            .setTotalOverflowCount(fields[14])
            .setMaxOverflowCount(fields[15])
            .setMinOverflowCount(fields[16])
            .setTotalPollsCount(fields[17])
            .setMaxPollsCount(fields[18])
            .setMinPollsCount(fields[19])
            .setTotalBusyDurationMs(fields[20])
            .setMaxBusyDurationMs(fields[21])
            .setMinBusyDurationMs(fields[22])
            .setTotalLocalQueueDepth(fields[23])
            .setMaxLocalQueueDepth(fields[24])
            .setMinLocalQueueDepth(fields[25])
            .setGlobalQueueDepth(fields[26])
            .setBlockingQueueDepth(fields[27])
            .build();
    }

    private DataFusionStatsProto.TaskMonitorMetrics buildTaskMonitorProto(
            DataFusionPluginStats.TaskMonitorValues tm) {
        return DataFusionStatsProto.TaskMonitorMetrics.newBuilder()
            .setTotalPollDurationMs(tm.getTotalPollDurationMs())
            .setTotalScheduledDurationMs(tm.getTotalScheduledDurationMs())
            .setTotalIdleDurationMs(tm.getTotalIdleDurationMs())
            .setTotalSlowPollCount(tm.getTotalSlowPollCount())
            .setTotalLongDelayCount(tm.getTotalLongDelayCount())
            .setSlowPollRatio(tm.getSlowPollRatio())
            .build();
    }

    private void assertRuntimeValuesMatch(DataFusionPluginStats.RuntimeValues rv, long[] fields) {
        assertEquals(fields[0], rv.getWorkersCount(), "workersCount mismatch");
        assertEquals(fields[1], rv.getTotalParkCount(), "totalParkCount mismatch");
        assertEquals(fields[2], rv.getMaxParkCount(), "maxParkCount mismatch");
        assertEquals(fields[3], rv.getMinParkCount(), "minParkCount mismatch");
        assertEquals(fields[4], rv.getTotalNoopCount(), "totalNoopCount mismatch");
        assertEquals(fields[5], rv.getMaxNoopCount(), "maxNoopCount mismatch");
        assertEquals(fields[6], rv.getMinNoopCount(), "minNoopCount mismatch");
        assertEquals(fields[7], rv.getTotalStealCount(), "totalStealCount mismatch");
        assertEquals(fields[8], rv.getMaxStealCount(), "maxStealCount mismatch");
        assertEquals(fields[9], rv.getMinStealCount(), "minStealCount mismatch");
        assertEquals(fields[10], rv.getTotalStealOperations(), "totalStealOperations mismatch");
        assertEquals(fields[11], rv.getTotalLocalScheduleCount(), "totalLocalScheduleCount mismatch");
        assertEquals(fields[12], rv.getMaxLocalScheduleCount(), "maxLocalScheduleCount mismatch");
        assertEquals(fields[13], rv.getMinLocalScheduleCount(), "minLocalScheduleCount mismatch");
        assertEquals(fields[14], rv.getTotalOverflowCount(), "totalOverflowCount mismatch");
        assertEquals(fields[15], rv.getMaxOverflowCount(), "maxOverflowCount mismatch");
        assertEquals(fields[16], rv.getMinOverflowCount(), "minOverflowCount mismatch");
        assertEquals(fields[17], rv.getTotalPollsCount(), "totalPollsCount mismatch");
        assertEquals(fields[18], rv.getMaxPollsCount(), "maxPollsCount mismatch");
        assertEquals(fields[19], rv.getMinPollsCount(), "minPollsCount mismatch");
        assertEquals(fields[20], rv.getTotalBusyDurationMs(), "totalBusyDurationMs mismatch");
        assertEquals(fields[21], rv.getMaxBusyDurationMs(), "maxBusyDurationMs mismatch");
        assertEquals(fields[22], rv.getMinBusyDurationMs(), "minBusyDurationMs mismatch");
        assertEquals(fields[23], rv.getTotalLocalQueueDepth(), "totalLocalQueueDepth mismatch");
        assertEquals(fields[24], rv.getMaxLocalQueueDepth(), "maxLocalQueueDepth mismatch");
        assertEquals(fields[25], rv.getMinLocalQueueDepth(), "minLocalQueueDepth mismatch");
        assertEquals(fields[26], rv.getGlobalQueueDepth(), "globalQueueDepth mismatch");
        assertEquals(fields[27], rv.getBlockingQueueDepth(), "blockingQueueDepth mismatch");
    }

    private void assertTaskMonitorEquals(DataFusionPluginStats.TaskMonitorValues expected,
                                          DataFusionPluginStats.TaskMonitorValues actual) {
        assertEquals(expected.getTotalPollDurationMs(), actual.getTotalPollDurationMs());
        assertEquals(expected.getTotalScheduledDurationMs(), actual.getTotalScheduledDurationMs());
        assertEquals(expected.getTotalIdleDurationMs(), actual.getTotalIdleDurationMs());
        assertEquals(expected.getTotalSlowPollCount(), actual.getTotalSlowPollCount());
        assertEquals(expected.getTotalLongDelayCount(), actual.getTotalLongDelayCount());
        assertEquals(expected.getSlowPollRatio(), actual.getSlowPollRatio(), 1e-10);
    }
}
