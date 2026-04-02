/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;
import org.opensearch.vectorized.execution.metrics.MetricProvider;
import org.opensearch.vectorized.execution.metrics.NativeMetricsSnapshot;

/**
 * Periodically collects native runtime metrics (Tokio CPU/IO) via the registered
 * {@link MetricProvider} and publishes them to {@link NativeMetricsSnapshot}.
 * <p>
 * Modeled after {@link ResourceUsageCollectorService}. The cached snapshot is read
 * by {@code TokioMetricsRejectionCheck} on the queue's offer path — no JNI on the
 * hot path.
 *
 * @opensearch.internal
 */
public class NativeMetricsCollectorService extends AbstractLifecycleComponent {

    private static final long REFRESH_INTERVAL_IN_MILLIS = 1000;
    private static final Logger logger = LogManager.getLogger(NativeMetricsCollectorService.class);

    private final ThreadPool threadPool;
    private volatile MetricProvider<DataFusionPluginStats> metricProvider;
    private volatile Scheduler.Cancellable scheduledFuture;

    public NativeMetricsCollectorService(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * Registers the metric provider for DataFusion stats collection.
     * Replaces any previously registered provider.
     */
    public void registerMetricProvider(MetricProvider<DataFusionPluginStats> provider) {
        this.metricProvider = provider;
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(
            this::collectMetrics,
            new TimeValue(REFRESH_INTERVAL_IN_MILLIS),
            ThreadPool.Names.GENERIC
        );
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() {}

    /**
     * Collects metrics from the registered provider and publishes a new snapshot.
     * Package-private so tests can call it directly.
     */
    void collectMetrics() {
        MetricProvider<DataFusionPluginStats> provider = this.metricProvider;
        if (provider == null) {
            return;
        }
        try {
            DataFusionPluginStats stats = provider.stats();
            NativeMetricsSnapshot.set(new NativeMetricsSnapshot(stats, System.currentTimeMillis()));
        } catch (Exception e) {
            logger.warn("Failed to collect native metrics", e);
            // retain previous cached snapshot
        }
    }
}
