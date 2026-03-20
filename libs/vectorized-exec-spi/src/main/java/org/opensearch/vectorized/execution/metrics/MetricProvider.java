/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

/**
 * Single-method interface for collecting metrics from a plugin.
 * Lives in SPI; implemented by each plugin in its own classloader.
 */
public interface MetricProvider {

    /**
     * Returns all plugin metrics as a single {@link PluginStats} object.
     */
    PluginStats stats();
}
