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
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link NativeExecutorTrackerRegistry}.
 *
 * Feature: tracker-registry-migration
 */
public class NativeExecutorTrackerRegistryTests {

    @BeforeProperty
    void setUp() {
        NativeExecutorTrackerRegistry.clear();
    }

    @AfterProperty
    void tearDown() {
        NativeExecutorTrackerRegistry.clear();
    }

    // --- Arbitraries ---

    @Provide
    Arbitrary<String> operationNames() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(50);
    }

    @Provide
    Arbitrary<Set<String>> distinctOperationNames() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(30)
            .set().ofMinSize(1).ofMaxSize(20);
    }

    // --- Property 1: getOrCreate idempotence + fresh tracker zero counters ---

    /**
     * Feature: tracker-registry-migration, Property 1: Registry getOrCreate idempotence
     * and fresh tracker initialization
     *
     * For any operation name string, calling getOrCreate(name) twice shall return the same
     * NativeExecutorTracker instance (reference equality), and a freshly created tracker
     * shall have getNativeInFlight() == 0 and getNativeAcquired() == 0.
     *
     * **Validates: Requirements 1.1, 1.2, 11.5**
     */
    @Property(tries = 100)
    void getOrCreateIsIdempotentAndFreshTrackerHasZeroCounters(@ForAll("operationNames") String name) {
        NativeExecutorTrackerRegistry.clear();
        NativeExecutorTracker first = NativeExecutorTrackerRegistry.getOrCreate(name);
        NativeExecutorTracker second = NativeExecutorTrackerRegistry.getOrCreate(name);

        assertSame(first, second, "getOrCreate must return the same instance for the same name");
        assertEquals(0, first.getNativeInFlight(), "fresh tracker nativeInFlight must be 0");
        assertEquals(0L, first.getNativeAcquired(), "fresh tracker nativeAcquired must be 0");
        assertEquals(name, first.getName(), "tracker name must match the requested name");
    }

    // --- Property 2: getAll reflects all registered trackers ---

    /**
     * Feature: tracker-registry-migration, Property 2: Registry getAll reflects all
     * registered trackers
     *
     * For any set of distinct operation name strings, after calling getOrCreate for each
     * name, getAll() shall return a collection whose size equals the number of distinct
     * names, and every returned tracker's getName() shall be present in the original name set.
     *
     * **Validates: Requirements 1.3**
     */
    @Property(tries = 100)
    void getAllReflectsAllRegisteredTrackers(@ForAll("distinctOperationNames") Set<String> names) {
        NativeExecutorTrackerRegistry.clear();
        for (String name : names) {
            NativeExecutorTrackerRegistry.getOrCreate(name);
        }

        Collection<NativeExecutorTracker> all = NativeExecutorTrackerRegistry.getAll();
        assertEquals(names.size(), all.size(), "getAll size must equal number of distinct names registered");

        Set<String> returnedNames = all.stream()
            .map(NativeExecutorTracker::getName)
            .collect(Collectors.toSet());
        assertEquals(names, returnedNames, "getAll tracker names must match the registered name set");
    }

    // --- Property 3: clear resets all state ---

    /**
     * Feature: tracker-registry-migration, Property 3: Registry clear resets all state
     *
     * For any set of registered trackers, after calling clear(), getAll() shall return an
     * empty collection, and a subsequent getOrCreate(name) shall return a new tracker
     * instance (not reference-equal to any pre-clear instance) with zero counters.
     *
     * **Validates: Requirements 1.4, 12.1, 12.2, 12.3**
     */
    @Property(tries = 100)
    void clearResetsAllState(@ForAll("distinctOperationNames") Set<String> names) {
        NativeExecutorTrackerRegistry.clear();
        // Register trackers and capture pre-clear references
        Set<NativeExecutorTracker> preClearTrackers = new HashSet<>();
        for (String name : names) {
            preClearTrackers.add(NativeExecutorTrackerRegistry.getOrCreate(name));
        }

        NativeExecutorTrackerRegistry.clear();

        assertTrue(NativeExecutorTrackerRegistry.getAll().isEmpty(),
            "getAll must return empty collection after clear");

        // Re-create trackers and verify they are new instances with zero counters
        for (String name : names) {
            NativeExecutorTracker fresh = NativeExecutorTrackerRegistry.getOrCreate(name);
            assertEquals(0, fresh.getNativeInFlight(), "post-clear tracker nativeInFlight must be 0");
            assertEquals(0L, fresh.getNativeAcquired(), "post-clear tracker nativeAcquired must be 0");

            for (NativeExecutorTracker old : preClearTrackers) {
                if (old.getName().equals(name)) {
                    assertNotSame(old, fresh,
                        "post-clear getOrCreate must return a new instance, not the pre-clear one");
                }
            }
        }
    }

    // --- Property 4: concurrent getOrCreate returns same instance ---

    /**
     * Feature: tracker-registry-migration, Property 4: Registry concurrent getOrCreate
     * returns same instance
     *
     * For any operation name, when getOrCreate(name) is called concurrently from N threads
     * (N >= 2), all threads shall receive the same NativeExecutorTracker instance (reference
     * equality), and getAll() shall contain exactly one tracker with that name.
     *
     * **Validates: Requirements 1.6**
     */
    @Property(tries = 100)
    void concurrentGetOrCreateReturnsSameInstance(@ForAll("operationNames") String name) throws Exception {
        NativeExecutorTrackerRegistry.clear();
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            List<Future<NativeExecutorTracker>> futures = executor.invokeAll(
                java.util.Collections.nCopies(threadCount, () -> NativeExecutorTrackerRegistry.getOrCreate(name))
            );

            NativeExecutorTracker expected = futures.get(0).get();
            for (Future<NativeExecutorTracker> future : futures) {
                assertSame(expected, future.get(),
                    "all concurrent getOrCreate calls must return the same instance");
            }

            Collection<NativeExecutorTracker> all = NativeExecutorTrackerRegistry.getAll();
            long matchCount = all.stream().filter(t -> t.getName().equals(name)).count();
            assertEquals(1, matchCount, "getAll must contain exactly one tracker with the given name");
        } finally {
            executor.shutdownNow();
        }
    }
}
