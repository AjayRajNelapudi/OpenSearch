/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static registry for {@link NativeExecutorTracker} instances, keyed by operation name.
 * <p>
 * Replaces constructor-based dependency injection of trackers through the plugin
 * constructor chain. Consumers resolve trackers at point of use via
 * {@link #getOrCreate(String)}, and stats collectors enumerate all registered
 * trackers via {@link #getAll()}.
 * <p>
 * Thread-safe: backed by {@link ConcurrentHashMap} with atomic
 * {@code computeIfAbsent} for tracker creation.
 *
 * @opensearch.internal
 */
public final class NativeExecutorTrackerRegistry {

    private static final ConcurrentHashMap<String, NativeExecutorTracker> TRACKERS = new ConcurrentHashMap<>();

    private NativeExecutorTrackerRegistry() {}

    /** Returns existing tracker or atomically creates a new one. */
    public static NativeExecutorTracker getOrCreate(String name) {
        return TRACKERS.computeIfAbsent(name, NativeExecutorTracker::new);
    }

    /** Returns unmodifiable view of all registered trackers. */
    public static Collection<NativeExecutorTracker> getAll() {
        return Collections.unmodifiableCollection(TRACKERS.values());
    }

    /** Removes all trackers. For test isolation only. */
    public static void clear() {
        TRACKERS.clear();
    }
}
