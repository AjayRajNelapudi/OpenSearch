/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! MetricsCollector reads cumulative runtime metrics directly from
//! `tokio::runtime::Handle::metrics()` (requires `tokio_unstable`).
//!
//! Returns a flat `[i64; RUNTIME_SIZE]` array using the layout constants from
//! `crate::metrics_layout`, ready for embedding into the JNI `jlongArray`.

use tokio::runtime::Handle;
use crate::metrics_layout;
use crate::runtime_manager::{MAX_IO_QUEUE_DEPTH, MAX_CPU_QUEUE_DEPTH};
use crate::queue_depth_histogram::QueueDepthHistogram;
use std::sync::atomic::{AtomicU64, Ordering};

/// Wraps a cloned `Handle` and reads cumulative metrics on demand.
pub struct MetricsCollector {
    handle: Handle,
    max_queue_depth: &'static AtomicU64,
    histogram: &'static QueueDepthHistogram,
}

impl MetricsCollector {
    /// Creates a new collector for the given runtime handle.
    pub fn new(handle: &Handle, max_queue_depth: &'static AtomicU64, histogram: &'static QueueDepthHistogram) -> Self {
        Self {
            handle: handle.clone(),
            max_queue_depth,
            histogram,
        }
    }

    /// Fallback when `tokio_unstable` is not set — returns zeroed metrics
    /// so the crate still compiles outside the normal Gradle/Cargo pipeline.
    #[cfg(not(tokio_unstable))]
    pub fn snapshot(&self) -> [i64; metrics_layout::RUNTIME_SIZE] {
        let mut arr = [0i64; metrics_layout::RUNTIME_SIZE];
        arr[metrics_layout::RUNTIME_MAX_GLOBAL_QUEUE_DEPTH] = self.max_queue_depth.load(Ordering::Relaxed) as i64;
        let (p50, p90, p99) = self.histogram.percentiles();
        arr[metrics_layout::RUNTIME_P50_GLOBAL_QUEUE_DEPTH] = p50 as i64;
        arr[metrics_layout::RUNTIME_P90_GLOBAL_QUEUE_DEPTH] = p90 as i64;
        arr[metrics_layout::RUNTIME_P99_GLOBAL_QUEUE_DEPTH] = p99 as i64;
        arr
    }

    #[cfg(tokio_unstable)]
    pub fn snapshot(&self) -> [i64; metrics_layout::RUNTIME_SIZE] {
        let m = self.handle.metrics();
        let n = m.num_workers();

        let mut total_overflow: u64 = 0;
        let mut total_polls: u64 = 0;
        let mut total_busy_ns: u128 = 0;
        let mut total_park: u64 = 0;
        let mut total_steal: u64 = 0;
        let mut total_noop: u64 = 0;
        let mut total_steal_ops: u64 = 0;
        let mut total_local_schedule: u64 = 0;
        let mut total_local_queue_depth: usize = 0;

        for i in 0..n {
            total_overflow += m.worker_overflow_count(i);
            total_polls += m.worker_poll_count(i);
            total_busy_ns += m.worker_total_busy_duration(i).as_nanos();
            total_park += m.worker_park_count(i);
            total_steal += m.worker_steal_count(i);
            total_noop += m.worker_noop_count(i);
            total_steal_ops += m.worker_steal_operations(i);
            total_local_schedule += m.worker_local_schedule_count(i);
            total_local_queue_depth += m.worker_local_queue_depth(i);
        }

        let (p50, p90, p99) = self.histogram.percentiles();

        let mut arr = [0i64; metrics_layout::RUNTIME_SIZE];
        arr[metrics_layout::RUNTIME_WORKERS_COUNT] = n as i64;
        arr[metrics_layout::RUNTIME_TOTAL_POLLS_COUNT] = total_polls as i64;
        arr[metrics_layout::RUNTIME_TOTAL_BUSY_DURATION_MS] = (total_busy_ns / 1_000_000) as i64;
        arr[metrics_layout::RUNTIME_TOTAL_OVERFLOW_COUNT] = total_overflow as i64;
        arr[metrics_layout::RUNTIME_GLOBAL_QUEUE_DEPTH] = m.global_queue_depth() as i64;
        arr[metrics_layout::RUNTIME_BLOCKING_QUEUE_DEPTH] = m.blocking_queue_depth() as i64;
        arr[metrics_layout::RUNTIME_MAX_GLOBAL_QUEUE_DEPTH] = self.max_queue_depth.load(Ordering::Relaxed) as i64;
        arr[metrics_layout::RUNTIME_P50_GLOBAL_QUEUE_DEPTH] = p50 as i64;
        arr[metrics_layout::RUNTIME_P90_GLOBAL_QUEUE_DEPTH] = p90 as i64;
        arr[metrics_layout::RUNTIME_P99_GLOBAL_QUEUE_DEPTH] = p99 as i64;
        arr[metrics_layout::RUNTIME_NUM_ALIVE_TASKS] = m.num_alive_tasks() as i64;
        arr[metrics_layout::RUNTIME_SPAWNED_TASKS_COUNT] = m.spawned_tasks_count() as i64;
        arr[metrics_layout::RUNTIME_REMOTE_SCHEDULE_COUNT] = m.remote_schedule_count() as i64;
        arr[metrics_layout::RUNTIME_BUDGET_FORCED_YIELD_COUNT] = m.budget_forced_yield_count() as i64;
        arr[metrics_layout::RUNTIME_NUM_BLOCKING_THREADS] = m.num_blocking_threads() as i64;
        arr[metrics_layout::RUNTIME_NUM_IDLE_BLOCKING_THREADS] = m.num_idle_blocking_threads() as i64;
        arr[metrics_layout::RUNTIME_TOTAL_PARK_COUNT] = total_park as i64;
        arr[metrics_layout::RUNTIME_TOTAL_STEAL_COUNT] = total_steal as i64;
        arr[metrics_layout::RUNTIME_TOTAL_NOOP_COUNT] = total_noop as i64;
        arr[metrics_layout::RUNTIME_TOTAL_STEAL_OPERATIONS] = total_steal_ops as i64;
        arr[metrics_layout::RUNTIME_TOTAL_LOCAL_SCHEDULE_COUNT] = total_local_schedule as i64;
        arr[metrics_layout::RUNTIME_TOTAL_LOCAL_QUEUE_DEPTH] = total_local_queue_depth as i64;
        arr
    }
}
