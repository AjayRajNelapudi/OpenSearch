/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Property-based tests for DataFusionPluginStats long[] decode using jqwik.
 *
 * Writeable round-trip and XContent tests have moved to NativeExecutorsStatsTests
 * in the server module, since DataFusionPluginStats is now a pure POJO.
 *
 * Feature: proto-to-longarray-migration
 * Feature: rust-layer-rejection
 */
public class DataFusionPluginStatsTests {

    // --- Arbitraries ---

    /**
     * Generates a valid long[40] array with non-zero cpu_runtime workers_count (index 10)
     * so that cpuRuntime is present after decode.
     */
    @Provide
    Arbitrary<long[]> validLong34WithCpu() {
        return Arbitraries.longs().between(1, Long.MAX_VALUE / 2)
            .array(long[].class).ofSize(40);
    }

    /**
     * Generates a valid long[40] array with cpu_runtime workers_count == 0 (index 10)
     * so that cpuRuntime is null after decode.
     */
    @Provide
    Arbitrary<long[]> validLong34WithoutCpu() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2)
            .array(long[].class).ofSize(40)
            .map(arr -> {
                // Zero out all cpu_runtime slots [10..19]
                for (int i = 10; i < 20; i++) {
                    arr[i] = 0;
                }
                return arr;
            });
    }

    // --- Property 1: long[] decode round-trip with CPU runtime present ---

    /**
     * Property 1: For any valid long[40] with non-zero cpu workers_count,
     * decode(long[]) produces an object whose field values match the input array.
     *
     * Feature: rust-layer-rejection, Property 2: long[40] decode round-trip
     * **Validates: Requirements 8.1, 8.3, 6.2**
     */
    @Property(tries = 100)
    void longArrayDecodeRoundTripWithCpu(@ForAll("validLong34WithCpu") long[] data) {
        DataFusionPluginStats decoded = DataFusionPluginStats.decode(data);

        // IO runtime [0..9]
        assertNotNull(decoded.getIoRuntime());
        assertRuntimeValues(decoded.getIoRuntime(), data, 0);

        // CPU runtime [10..19] — present because workers_count > 0
        assertNotNull(decoded.getCpuRuntime(), "cpuRuntime should be present when workers_count > 0");
        assertRuntimeValues(decoded.getCpuRuntime(), data, 10);

        // Task monitors [20..39] with stride 4
        assertTaskMonitorValues(decoded.getQueryExecution(), data, 20);
        assertTaskMonitorValues(decoded.getStreamNext(), data, 24);
        assertTaskMonitorValues(decoded.getFetchPhase(), data, 28);
        assertTaskMonitorValues(decoded.getSegmentStats(), data, 32);
        assertTaskMonitorValues(decoded.getIndexedQueryExecution(), data, 36);
    }

    // --- Property 2: absent CPU runtime when workers_count == 0 ---

    /**
     * Property 2: When cpu_runtime workers_count (index 6) is 0 and all cpu slots
     * are zero, decoded cpuRuntime is null while all other fields decode correctly.
     *
     * **Validates: Requirements 2.3**
     */
    @Property(tries = 50)
    void absentCpuRuntimeWhenWorkersCountZero(@ForAll("validLong34WithoutCpu") long[] data) {
        DataFusionPluginStats decoded = DataFusionPluginStats.decode(data);

        assertNull(decoded.getCpuRuntime(), "cpuRuntime should be null when workers_count == 0");
        assertNotNull(decoded.getIoRuntime(), "ioRuntime should still be present");
        assertRuntimeValues(decoded.getIoRuntime(), data, 0);

        // Task monitors still decode correctly with stride 4
        assertTaskMonitorValues(decoded.getQueryExecution(), data, 20);
        assertTaskMonitorValues(decoded.getStreamNext(), data, 24);
        assertTaskMonitorValues(decoded.getFetchPhase(), data, 28);
        assertTaskMonitorValues(decoded.getSegmentStats(), data, 32);
        assertTaskMonitorValues(decoded.getIndexedQueryExecution(), data, 36);
    }

    // --- Property 3: invalid array length throws ---

    /**
     * Property 3: Passing an array whose length is not 40 to decode() throws
     * IllegalArgumentException.
     *
     * Feature: rust-layer-rejection, Property 3: Invalid array length rejection
     * **Validates: Requirements 8.2**
     */
    @Property(tries = 50)
    void invalidArrayLengthThrows(@ForAll("invalidLength") long[] data) {
        assertThrows(IllegalArgumentException.class, () -> DataFusionPluginStats.decode(data),
            "decode() should throw IllegalArgumentException for array length != 40");
    }

    @Provide
    Arbitrary<long[]> invalidLength() {
        // Generate arrays of length 0..50 but exclude 40
        return Arbitraries.integers().between(0, 50)
            .filter(len -> len != 40)
            .flatMap(len -> Arbitraries.longs().between(0, Long.MAX_VALUE / 2)
                .array(long[].class).ofSize(len));
    }

    // --- Property 4: null array throws ---

    /**
     * Edge case: Passing null to decode() throws IllegalArgumentException.
     *
     * **Validates: Requirements 8.2**
     */
    @Property(tries = 1)
    void nullArrayThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> DataFusionPluginStats.decode(null),
            "decode() should throw IllegalArgumentException for null array");
    }

    // --- Property 6: TaskMonitorValues equality includes rejected ---

    /**
     * Feature: rust-layer-rejection, Property 6: TaskMonitorValues equality includes rejected
     *
     * For any two TaskMonitorValues with identical poll, scheduled, and idle durations
     * but different rejected counts, equals() shall return false and hashCode() should differ.
     *
     * **Validates: Requirements 6.6**
     */
    @Property(tries = 100)
    void taskMonitorValuesEqualityIncludesRejected(
        @ForAll("positiveLong") long poll,
        @ForAll("positiveLong") long scheduled,
        @ForAll("positiveLong") long idle,
        @ForAll("distinctRejectedPair") long[] rejectedPair
    ) {
        DataFusionPluginStats.TaskMonitorValues a = new DataFusionPluginStats.TaskMonitorValues(
            poll, scheduled, idle, rejectedPair[0]
        );
        DataFusionPluginStats.TaskMonitorValues b = new DataFusionPluginStats.TaskMonitorValues(
            poll, scheduled, idle, rejectedPair[1]
        );

        assertNotEquals(a, b, "TaskMonitorValues with different rejected counts should not be equal");
        assertNotEquals(a.hashCode(), b.hashCode(),
            "TaskMonitorValues with different rejected counts should have different hashCodes");
    }

    @Provide
    Arbitrary<long[]> distinctRejectedPair() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2)
            .array(long[].class).ofSize(2)
            .filter(arr -> arr[0] != arr[1]);
    }

    @Provide
    Arbitrary<Long> positiveLong() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
    }

    // --- Helper methods ---

    private void assertRuntimeValues(DataFusionPluginStats.RuntimeValues rv, long[] data, int offset) {
        assertEquals(data[offset], rv.getWorkersCount(), "workersCount mismatch at offset " + offset);
        assertEquals(data[offset + 1], rv.getTotalPollsCount(), "totalPollsCount mismatch");
        assertEquals(data[offset + 2], rv.getTotalBusyDurationMs(), "totalBusyDurationMs mismatch");
        assertEquals(data[offset + 3], rv.getTotalOverflowCount(), "totalOverflowCount mismatch");
        assertEquals(data[offset + 4], rv.getGlobalQueueDepth(), "globalQueueDepth mismatch");
        assertEquals(data[offset + 5], rv.getBlockingQueueDepth(), "blockingQueueDepth mismatch");
        assertEquals(data[offset + 6], rv.getMaxGlobalQueueDepth(), "maxGlobalQueueDepth mismatch");
        assertEquals(data[offset + 7], rv.getP50GlobalQueueDepth(), "p50GlobalQueueDepth mismatch");
        assertEquals(data[offset + 8], rv.getP90GlobalQueueDepth(), "p90GlobalQueueDepth mismatch");
        assertEquals(data[offset + 9], rv.getP99GlobalQueueDepth(), "p99GlobalQueueDepth mismatch");
    }

    private void assertTaskMonitorValues(DataFusionPluginStats.TaskMonitorValues tm, long[] data, int offset) {
        assertEquals(data[offset], tm.getTotalPollDurationMs(), "totalPollDurationMs mismatch at offset " + offset);
        assertEquals(data[offset + 1], tm.getTotalScheduledDurationMs(), "totalScheduledDurationMs mismatch");
        assertEquals(data[offset + 2], tm.getTotalIdleDurationMs(), "totalIdleDurationMs mismatch");
        assertEquals(data[offset + 3], tm.getRejected(), "rejected mismatch at offset " + (offset + 3));
    }
}
