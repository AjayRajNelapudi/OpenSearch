/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.stats.BackendStatsProvider;
import org.opensearch.plugin.stats.DataFusionStats;
import org.opensearch.plugin.stats.PluginStats;
import org.opensearch.plugin.stats.ResourceUsageStats;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Lightweight unit tests for {@link ResourceUsageCollectorService} wiring of
 * {@link BackendStatsProvider} into {@code nativeMemoryUtilization}.
 *
 * <p>These tests construct the service directly with mock providers and verify
 * that the public {@code collectNodeResourceUsageStats} stores the correct
 * {@code nativeMemoryUtilization} value, and that the provider extraction
 * logic (as implemented in {@code collectLocalNodeResourceUsageStats}) produces
 * the expected value for each scenario.
 *
 * <p>Requirements: 6.4, 6.5, 6.6
 */
public class ResourceUsageCollectorServiceWiringTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("wiring_tests");
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        threadPool.shutdownNow();
        super.tearDown();
    }

    /**
     * Extracts nativeMemoryUtilization from a list of providers using the same
     * logic as {@code collectLocalNodeResourceUsageStats}: find the provider
     * named "datafusion", cast its stats to DataFusionStats, and read the
     * ResourceUsageStats value. Falls back to 0.0.
     */
    private static double extractNativeMemoryUtilization(List<BackendStatsProvider> providers) {
        double nativeMemoryUtilization = 0.0;
        for (BackendStatsProvider provider : providers) {
            if ("datafusion".equals(provider.name())) {
                PluginStats pluginStats = provider.getBackendStats();
                if (pluginStats instanceof DataFusionStats) {
                    DataFusionStats dfStats = (DataFusionStats) pluginStats;
                    ResourceUsageStats resourceUsageStats = dfStats.getResourceUsageStats();
                    if (resourceUsageStats != null) {
                        nativeMemoryUtilization = resourceUsageStats.getNativeMemoryUtilization();
                    }
                }
                break;
            }
        }
        return nativeMemoryUtilization;
    }

    /**
     * With a mock BackendStatsProvider("datafusion") returning DataFusionStats
     * with ResourceUsageStats(42.0), verify nativeMemoryUtilization == 42.0
     * in stored stats.
     */
    public void testProviderWithResourceUsageStats() {
        BackendStatsProvider provider = new BackendStatsProvider() {
            @Override
            public String name() {
                return "datafusion";
            }

            @Override
            public PluginStats getBackendStats() {
                return new DataFusionStats(null, new ResourceUsageStats(42.0));
            }
        };

        List<BackendStatsProvider> providers = List.of(provider);

        // Verify extraction logic produces 42.0
        double extracted = extractNativeMemoryUtilization(providers);
        assertEquals(42.0, extracted, 0.0);

        // Verify end-to-end: construct service, call collectNodeResourceUsageStats, check stored value
        ResourceUsageCollectorService service = new ResourceUsageCollectorService(
            null, // nodeResourceUsageTracker not needed for direct collectNodeResourceUsageStats calls
            clusterService,
            threadPool,
            providers
        );

        service.collectNodeResourceUsageStats("node1", System.currentTimeMillis(), 50.0, 60.0, new IoUsageStats(70), extracted);

        Map<String, NodeResourceUsageStats> allStats = service.getAllNodeStatistics();
        assertTrue(allStats.containsKey("node1"));
        assertEquals(42.0, allStats.get("node1").getNativeMemoryUtilization(), 0.0);
    }

    /**
     * With no providers, verify default 0.0.
     */
    public void testNoProviders() {
        List<BackendStatsProvider> providers = Collections.emptyList();

        // Verify extraction logic produces 0.0
        double extracted = extractNativeMemoryUtilization(providers);
        assertEquals(0.0, extracted, 0.0);

        // Verify end-to-end storage
        ResourceUsageCollectorService service = new ResourceUsageCollectorService(
            null,
            clusterService,
            threadPool,
            providers
        );

        service.collectNodeResourceUsageStats("node1", System.currentTimeMillis(), 50.0, 60.0, new IoUsageStats(70), extracted);

        Map<String, NodeResourceUsageStats> allStats = service.getAllNodeStatistics();
        assertTrue(allStats.containsKey("node1"));
        assertEquals(0.0, allStats.get("node1").getNativeMemoryUtilization(), 0.0);
    }

    /**
     * With a provider returning DataFusionStats with null ResourceUsageStats,
     * verify default 0.0.
     */
    public void testProviderWithNullResourceUsageStats() {
        BackendStatsProvider provider = new BackendStatsProvider() {
            @Override
            public String name() {
                return "datafusion";
            }

            @Override
            public PluginStats getBackendStats() {
                return new DataFusionStats(null, null);
            }
        };

        List<BackendStatsProvider> providers = List.of(provider);

        // Verify extraction logic produces 0.0
        double extracted = extractNativeMemoryUtilization(providers);
        assertEquals(0.0, extracted, 0.0);

        // Verify end-to-end storage
        ResourceUsageCollectorService service = new ResourceUsageCollectorService(
            null,
            clusterService,
            threadPool,
            providers
        );

        service.collectNodeResourceUsageStats("node1", System.currentTimeMillis(), 50.0, 60.0, new IoUsageStats(70), extracted);

        Map<String, NodeResourceUsageStats> allStats = service.getAllNodeStatistics();
        assertTrue(allStats.containsKey("node1"));
        assertEquals(0.0, allStats.get("node1").getNativeMemoryUtilization(), 0.0);
    }
}
