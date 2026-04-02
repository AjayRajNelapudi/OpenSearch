/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link NativeExecutorTracker}.
 */
public class NativeInflightTrackerPropertyTests {

    // Feature: native-inflight-rejection, Property 1: Acquire/release round-trip under concurrency

    /**
     * Property 1: For any T threads × N acquire/release pairs, if every acquire()
     * is followed by exactly one release(), getNativeInFlight() == 0 after all complete.
     */
    @Property(tries = 100)
    void acquireReleaseRoundTripUnderConcurrency(
        @ForAll @IntRange(min = 1, max = 20) int numThreads,
        @ForAll @IntRange(min = 1, max = 100) int pairsPerThread
    ) throws InterruptedException {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test");
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < pairsPerThread; i++) {
                        tracker.acquire();
                        tracker.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean finished = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(true, finished, "All threads should complete within timeout");
        assertEquals(0, tracker.getNativeInFlight(),
            "After all threads complete with paired acquire/release, in-flight count must be zero");
    }

    // Feature: native-inflight-rejection, Property 2: Non-negativity invariant

    /**
     * Property 2: getNativeInFlight() never goes negative, even with extra releases.
     */
    @Property(tries = 100)
    void nonNegativityInvariant(
        @ForAll @Size(min = 1, max = 200) List<Boolean> operations
    ) {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test");

        for (Boolean isAcquire : operations) {
            if (isAcquire) {
                tracker.acquire();
            } else {
                tracker.release();
            }
            assertTrue(tracker.getNativeInFlight() >= 0,
                "getNativeInFlight() must never be negative, but was: " + tracker.getNativeInFlight());
        }
    }

    // Feature: native-inflight-rejection, Property 7: Acquired counter monotonicity

    /**
     * Property 7: getNativeAcquired() is monotonically non-decreasing and equals
     * the total number of acquire() calls.
     */
    @Property(tries = 100)
    void acquiredCounterMonotonicity(
        @ForAll @Size(min = 1, max = 200) List<Boolean> operations
    ) {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test");

        long previousAcquired = 0;
        int acquireCount = 0;

        for (Boolean isAcquire : operations) {
            if (isAcquire) {
                tracker.acquire();
                acquireCount++;
            } else {
                tracker.release();
            }

            long currentAcquired = tracker.getNativeAcquired();
            assertTrue(currentAcquired >= previousAcquired,
                "getNativeAcquired() must be monotonically non-decreasing");
            previousAcquired = currentAcquired;
        }

        assertEquals(acquireCount, tracker.getNativeAcquired(),
            "getNativeAcquired() must equal the total number of acquire() calls");
    }

    // Feature: native-inflight-rejection, Property 10: Conservation invariant

    /**
     * Property 10: getNativeAcquired() == getNativeInFlight() + successfulReleases.
     */
    @Property(tries = 100)
    void conservationInvariant(
        @ForAll @Size(min = 1, max = 200) List<Boolean> operations
    ) {
        NativeExecutorTracker tracker = new NativeExecutorTracker("test");

        int successfulReleases = 0;
        int currentlyHeld = 0;

        for (Boolean isAcquire : operations) {
            if (isAcquire) {
                tracker.acquire();
                currentlyHeld++;
            } else {
                if (currentlyHeld > 0) {
                    tracker.release();
                    currentlyHeld--;
                    successfulReleases++;
                } else {
                    tracker.release(); // extra release, clamped at zero
                }
            }
        }

        assertEquals(tracker.getNativeAcquired(), tracker.getNativeInFlight() + successfulReleases,
            "Conservation invariant: getNativeAcquired() must equal getNativeInFlight() + successfulReleases");
    }
}
