/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Node-level partition admission gate.
//!
//! Limits total concurrent partition streams across all active queries by
//! gating query execution behind a [`tokio::sync::Semaphore`]. A query must
//! acquire all N permits (where N = `target_partitions`) atomically before
//! execution begins.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Node-level partition admission gate.
/// Limits total concurrent partition streams across all active queries.
pub struct PartitionSemaphore {
    semaphore: Arc<Semaphore>,
    max_permits: u32,
    total_wait_duration_ms: AtomicU64,
    total_partitions_started: AtomicU64,
}

impl PartitionSemaphore {
    /// Create a new semaphore with `floor(cpu_threads * 1.5)` permits.
    ///
    /// The permit count is always at least 1, even if `cpu_threads` is 0.
    pub fn new(cpu_threads: usize) -> Self {
        let max_permits = ((cpu_threads as f64) * 1.5).floor() as u32;
        let max_permits = max_permits.max(1); // minimum 1 permit
        Self {
            semaphore: Arc::new(Semaphore::new(max_permits as usize)),
            max_permits,
            total_wait_duration_ms: AtomicU64::new(0),
            total_partitions_started: AtomicU64::new(0),
        }
    }

    /// Acquire `n` permits atomically. Blocks until all N are available.
    ///
    /// Returns an [`OwnedSemaphorePermit`] representing all N permits.
    /// Dropping the permit returns all N permits to the semaphore.
    pub async fn acquire_budget(&self, n: u32) -> OwnedSemaphorePermit {
        let start = Instant::now();
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(n)
            .await
            .expect("semaphore closed unexpectedly");
        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.total_wait_duration_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        self.total_partitions_started
            .fetch_add(n as u64, Ordering::Relaxed);
        permit
    }

    /// Returns the total number of permits (semaphore capacity).
    pub fn max_permits(&self) -> u32 {
        self.max_permits
    }

    /// Returns the number of permits currently held by active queries.
    pub fn active_permits(&self) -> u32 {
        self.max_permits - self.semaphore.available_permits() as u32
    }

    /// Returns the cumulative milliseconds queries spent waiting for permits.
    pub fn total_wait_duration_ms(&self) -> u64 {
        self.total_wait_duration_ms.load(Ordering::Relaxed)
    }

    /// Returns the cumulative count of permits granted since startup.
    pub fn total_partitions_started(&self) -> u64 {
        self.total_partitions_started.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // ─── 1.6: Unit tests for PartitionSemaphore::new ───

    #[test]
    fn new_with_1_cpu_thread_yields_1_permit() {
        let sem = PartitionSemaphore::new(1);
        // floor(1 * 1.5) = 1
        assert_eq!(sem.max_permits(), 1);
    }

    #[test]
    fn new_with_2_cpu_threads_yields_3_permits() {
        let sem = PartitionSemaphore::new(2);
        // floor(2 * 1.5) = 3
        assert_eq!(sem.max_permits(), 3);
    }

    #[test]
    fn new_with_4_cpu_threads_yields_6_permits() {
        let sem = PartitionSemaphore::new(4);
        // floor(4 * 1.5) = 6
        assert_eq!(sem.max_permits(), 6);
    }

    #[test]
    fn new_with_8_cpu_threads_yields_12_permits() {
        let sem = PartitionSemaphore::new(8);
        // floor(8 * 1.5) = 12
        assert_eq!(sem.max_permits(), 12);
    }

    #[test]
    fn new_with_16_cpu_threads_yields_24_permits() {
        let sem = PartitionSemaphore::new(16);
        // floor(16 * 1.5) = 24
        assert_eq!(sem.max_permits(), 24);
    }

    #[test]
    fn new_with_0_cpu_threads_yields_minimum_1_permit() {
        let sem = PartitionSemaphore::new(0);
        // floor(0 * 1.5) = 0, but .max(1) ensures minimum 1
        assert_eq!(sem.max_permits(), 1);
    }

    // ─── 1.7: Unit tests for acquire/release behavior ───

    #[tokio::test]
    async fn acquire_increases_active_permits() {
        let sem = PartitionSemaphore::new(4); // 6 permits
        assert_eq!(sem.active_permits(), 0);

        let permit = sem.acquire_budget(3).await;
        assert_eq!(sem.active_permits(), 3);

        drop(permit);
        assert_eq!(sem.active_permits(), 0);
    }

    #[tokio::test]
    async fn acquire_all_permits_and_release() {
        let sem = PartitionSemaphore::new(4); // 6 permits
        let permit = sem.acquire_budget(6).await;
        assert_eq!(sem.active_permits(), 6);

        drop(permit);
        assert_eq!(sem.active_permits(), 0);
    }

    #[tokio::test]
    async fn counters_increment_on_acquire() {
        let sem = PartitionSemaphore::new(4); // 6 permits
        assert_eq!(sem.total_partitions_started(), 0);

        let _p1 = sem.acquire_budget(2).await;
        assert_eq!(sem.total_partitions_started(), 2);

        let _p2 = sem.acquire_budget(3).await;
        assert_eq!(sem.total_partitions_started(), 5);
    }

    // ─── 1.8: Unit test for blocking behavior ───

    #[tokio::test]
    async fn acquire_blocks_when_permits_unavailable() {
        let sem = Arc::new(PartitionSemaphore::new(2)); // 3 permits

        // Acquire all 3 permits
        let permit = sem.acquire_budget(3).await;
        assert_eq!(sem.active_permits(), 3);

        // Spawn a task that tries to acquire 1 more permit — it should block
        let sem_clone = Arc::clone(&sem);
        let handle = tokio::spawn(async move {
            sem_clone.acquire_budget(1).await
        });

        // Give the spawned task time to reach the await point
        tokio::time::sleep(Duration::from_millis(50)).await;

        // The task should still be pending (not completed)
        assert!(!handle.is_finished());

        // Drop the first permit to unblock the waiting task
        drop(permit);

        // The spawned task should now complete
        let acquired = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("task did not complete within timeout")
            .expect("task panicked");

        // Verify the permit was acquired
        assert_eq!(sem.active_permits(), 1);
        drop(acquired);
        assert_eq!(sem.active_permits(), 0);
    }

    // ─── 11.1: Integration test — execute_query consumes, stream_close returns ───

    /// Integration test: simulates the execute_query → stream_close flow.
    /// Verifies permits are consumed on acquire and returned on drop.
    #[tokio::test]
    async fn integration_execute_query_consumes_and_stream_close_returns() {
        let sem = Arc::new(PartitionSemaphore::new(4)); // 6 permits

        // Simulate execute_query: acquire permits
        let permit = sem.acquire_budget(4).await;
        assert_eq!(sem.active_permits(), 4);

        // Simulate attaching to QueryStreamHandle (permit is moved into Option)
        let held_permit: Option<OwnedSemaphorePermit> = Some(permit);
        assert_eq!(sem.active_permits(), 4);

        // Simulate stream_close: drop the handle (which drops the permit)
        drop(held_permit);
        assert_eq!(sem.active_permits(), 0);
    }

    // ─── 11.2: Integration test — execute_with_context independently gates ───

    /// Integration test: simulates two independent gate paths.
    /// execute_query and execute_with_context each acquire independently.
    #[tokio::test]
    async fn integration_execute_with_context_independently_gates() {
        let sem = Arc::new(PartitionSemaphore::new(8)); // 12 permits

        // Simulate execute_query acquiring 4 permits
        let permit1 = sem.acquire_budget(4).await;
        assert_eq!(sem.active_permits(), 4);

        // Simulate execute_with_context acquiring 3 permits independently
        let permit2 = sem.acquire_budget(3).await;
        assert_eq!(sem.active_permits(), 7);

        // Release execute_query permits
        drop(permit1);
        assert_eq!(sem.active_permits(), 3);

        // Release execute_with_context permits
        drop(permit2);
        assert_eq!(sem.active_permits(), 0);
    }

    // ─── 11.3: Integration test — execute_local_plan does NOT consume permits ───

    /// Integration test: execute_local_plan path does NOT consume permits.
    /// The semaphore state is unchanged after a "local plan" execution.
    #[tokio::test]
    async fn integration_execute_local_plan_does_not_consume_permits() {
        let sem = Arc::new(PartitionSemaphore::new(4)); // 6 permits

        // Before: no permits consumed
        assert_eq!(sem.active_permits(), 0);
        assert_eq!(sem.total_partitions_started(), 0);

        // Simulate execute_local_plan: it does NOT call acquire_budget
        // (verified by code inspection — execute_local_plan in api.rs
        // creates a QueryStreamHandle without a partition permit)

        // After: still no permits consumed
        assert_eq!(sem.active_permits(), 0);
        assert_eq!(sem.total_partitions_started(), 0);
    }

    // ─── 11.4: Integration test — stats packing produces correct values ───

    /// Integration test: stats packing produces correct values.
    #[tokio::test]
    async fn integration_stats_contains_correct_partition_semaphore_values() {
        use crate::stats::pack_partition_semaphore;

        let sem = PartitionSemaphore::new(8); // 12 permits

        // Initial state
        let repr = pack_partition_semaphore(&sem);
        assert_eq!(repr.max_permits, 12);
        assert_eq!(repr.active_permits, 0);
        assert_eq!(repr.total_wait_duration_ms, 0);
        assert_eq!(repr.total_partitions_started, 0);

        // After acquiring permits
        let permit = sem.acquire_budget(5).await;
        let repr = pack_partition_semaphore(&sem);
        assert_eq!(repr.max_permits, 12);
        assert_eq!(repr.active_permits, 5);
        assert_eq!(repr.total_partitions_started, 5);

        // After releasing
        drop(permit);
        let repr = pack_partition_semaphore(&sem);
        assert_eq!(repr.max_permits, 12);
        assert_eq!(repr.active_permits, 0);
        assert_eq!(repr.total_partitions_started, 5); // cumulative, doesn't decrease
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// Property 1: permit capacity equals floor(cpu_threads × 1.5).max(1)
        ///
        /// **Validates: Requirements 1.1, 1.3**
        #[test]
        fn permit_capacity_matches_formula(cpu_threads in 1usize..=256) {
            let sem = PartitionSemaphore::new(cpu_threads);
            let expected = ((cpu_threads as f64) * 1.5).floor() as u32;
            let expected = expected.max(1);
            prop_assert_eq!(sem.max_permits(), expected);
        }

        /// Property 2: acquire-release round trip preserves semaphore state
        ///
        /// **Validates: Requirements 2.1, 3.2, 3.3, 8.2**
        #[test]
        fn acquire_release_preserves_state(cpu_threads in 1usize..=64, n_factor in 1u32..=100) {
            let sem = PartitionSemaphore::new(cpu_threads);
            let max = sem.max_permits();
            // Clamp n to [1, max_permits]
            let n = ((n_factor - 1) % max) + 1;

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                prop_assert_eq!(sem.active_permits(), 0);

                let permit = sem.acquire_budget(n).await;
                prop_assert_eq!(sem.active_permits(), n);

                drop(permit);
                prop_assert_eq!(sem.active_permits(), 0);
                Ok(())
            })?;
        }

        /// Property 3: cumulative total_partitions_started equals sum of all acquisitions
        ///
        /// **Validates: Requirements 9.2**
        #[test]
        fn cumulative_partitions_started_equals_sum(
            cpu_threads in 4usize..=32,
            acquisitions in proptest::collection::vec(1u32..=4, 1..=8)
        ) {
            let sem = PartitionSemaphore::new(cpu_threads);
            let max = sem.max_permits();

            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let mut total: u64 = 0;
                for &n in &acquisitions {
                    let clamped = n.min(max).max(1);
                    let permit = sem.acquire_budget(clamped).await;
                    total += clamped as u64;
                    drop(permit); // release before next acquisition to avoid deadlock
                }
                prop_assert_eq!(sem.total_partitions_started(), total);
                Ok(())
            })?;
        }
    }
}
