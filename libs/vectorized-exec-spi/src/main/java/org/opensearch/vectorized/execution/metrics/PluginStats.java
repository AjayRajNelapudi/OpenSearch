/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;

/**
 * Interface for plugin-provided stats.
 *
 * NamedWriteable is required (not plain Writeable) because NodeStats
 * holds a PluginStats interface reference — the server doesn't know
 * the concrete class at compile time. The NamedWriteableRegistry maps
 * the writeable name to the plugin's constructor at runtime.
 *
 * Existing stats (OsStats, JvmStats, etc.) use plain Writeable because
 * NodeStats knows each concrete type. PluginStats is polymorphic —
 * same pattern as QueryBuilder, AggregationBuilder, Task.Status.
 */
public interface PluginStats extends NamedWriteable, ToXContentFragment {
}
