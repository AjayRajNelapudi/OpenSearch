/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.threadpool;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTracker;
import org.opensearch.vectorized.execution.metrics.NativeExecutorTrackerRegistry;

import java.util.concurrent.BlockingQueue;

/**
 * Unit tests verifying that the search thread pool is wired with a
 * {@code CompositeResizableBlockingQueue} and that the registry-sourced tracker injection works correctly.
 */
public class NativeInflightQueueWiringTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        NativeExecutorTrackerRegistry.clear();
    }

    @Override
    public void tearDown() throws Exception {
        NativeExecutorTrackerRegistry.clear();
        super.tearDown();
    }

    public void testSearchPoolUsesNativeInflightAwareQueue() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test-native-wiring");
        try {
            OpenSearchThreadPoolExecutor searchExecutor = (OpenSearchThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
            BlockingQueue<Runnable> queue = searchExecutor.getQueue();
            assertEquals("CompositeResizableBlockingQueue", queue.getClass().getSimpleName());

            // Register a tracker via the registry — queue reads from registry automatically
            NativeExecutorTrackerRegistry.getOrCreate("test_op");
        } finally {
            terminate(threadPool);
        }
    }

    public void testSearchPoolBehavesNormallyWithoutTracker() throws Exception {
        Settings settings = Settings.builder().put("thread_pool.search.queue_size", 5).build();
        TestThreadPool threadPool = new TestThreadPool("test-no-tracker", settings);
        try {
            OpenSearchThreadPoolExecutor searchExecutor = (OpenSearchThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
            BlockingQueue<Runnable> queue = searchExecutor.getQueue();
            assertEquals("CompositeResizableBlockingQueue", queue.getClass().getSimpleName());

            int accepted = 0;
            for (int i = 0; i < 5; i++) {
                if (queue.offer(() -> {})) accepted++;
            }
            assertEquals("Queue should accept exactly 5 tasks with empty registry and capacity 5", 5, accepted);
            assertFalse("offer() must fail when queue is at capacity", queue.offer(() -> {}));
        } finally {
            terminate(threadPool);
        }
    }

    public void testSearchPoolRejectsWhenNativeInflightExceedsCapacity() throws Exception {
        Settings settings = Settings.builder().put("thread_pool.search.queue_size", 10).build();
        TestThreadPool threadPool = new TestThreadPool("test-rejection-wiring", settings);
        try {
            OpenSearchThreadPoolExecutor searchExecutor = (OpenSearchThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
            BlockingQueue<Runnable> queue = searchExecutor.getQueue();

            // Register tracker via registry instead of threadPool.setNativeExecutorTrackersForSearch()
            NativeExecutorTracker tracker = NativeExecutorTrackerRegistry.getOrCreate("test_op");

            for (int i = 0; i < 10; i++) {
                tracker.acquire();
            }

            assertFalse("offer() must return false when apparent size (nativeInFlight) >= capacity", queue.offer(() -> {}));

            for (int i = 0; i < 10; i++) {
                tracker.release();
            }

            assertTrue("offer() must succeed after releasing all native in-flight permits", queue.offer(() -> {}));
        } finally {
            terminate(threadPool);
        }
    }

    public void testNonSearchPoolUsesStandardResizableBlockingQueue() throws Exception {
        TestThreadPool threadPool = new TestThreadPool("test-non-search");
        try {
            OpenSearchThreadPoolExecutor throttledExecutor =
                (OpenSearchThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH_THROTTLED);
            BlockingQueue<Runnable> queue = throttledExecutor.getQueue();
            assertEquals("ResizableBlockingQueue", queue.getClass().getSimpleName());
        } finally {
            terminate(threadPool);
        }
    }
}
