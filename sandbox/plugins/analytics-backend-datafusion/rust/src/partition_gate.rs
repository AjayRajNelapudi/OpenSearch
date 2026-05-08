/*
 * SPDX-License-Identifier: Apache-2.0
 */

//! Node-level partition budget gate.
//!
//! Limits the number of concurrent `stream_next` batch fetches across all
//! active queries on the node using a [`tokio::sync::Semaphore`].
//!
//! The gate is initialized with `floor(cpu_threads × 1.5)` permits. Each
//! `stream_next` call acquires one permit before polling the stream and
//! releases it when the batch is returned (or the stream ends).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Node-level partition budget gate.
///
/// Limits concurrent `stream_next` batch fetches by requiring callers to
/// acquire a permit before polling. Permits are released on drop.
pub struct PartitionGate {
    semaphore: Arc<Semaphore>,
    max_permits: u32,
    total_wait_duration_ms: AtomicU64,
    total_batches_started: AtomicU64,
}

impl PartitionGate {
    /// Create a new `PartitionGate` with `floor(cpu_threads × 1.5).max(1)` permits.
    pub fn new(cpu_threads: usize) -> Self {
        let max_permits = ((cpu_threads as f64) * 1.5).floor() as u32;
        let max_permits = max_permits.max(1);
        Self {
            semaphore: Arc::new(Semaphore::new(max_permits as usize)),
            max_permits,
            total_wait_duration_ms: AtomicU64::new(0),
            total_batches_started: AtomicU64::new(0),
        }
    }

    /// Acquire one permit. Blocks (async) until a permit is available.
    ///
    /// Returns an [`OwnedSemaphorePermit`] guard that releases the permit on
    /// drop. Also records the wait duration and increments the batches-started
    /// counter.
    pub async fn acquire(&self) -> OwnedSemaphorePermit {
        let start = Instant::now();
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("partition gate semaphore closed");
        let elapsed_ms = start.elapsed().as_millis() as u64;
        self.total_wait_duration_ms
            .fetch_add(elapsed_ms, Ordering::Relaxed);
        self.total_batches_started.fetch_add(1, Ordering::Relaxed);
        permit
    }

    /// Total number of permits (immutable after construction).
    pub fn max_permits(&self) -> u32 {
        self.max_permits
    }

    /// Number of permits currently held by active `stream_next` calls.
    pub fn active_permits(&self) -> u32 {
        self.max_permits - self.semaphore.available_permits() as u32
    }

    /// Cumulative milliseconds spent waiting for permits.
    pub fn total_wait_duration_ms(&self) -> u64 {
        self.total_wait_duration_ms.load(Ordering::Relaxed)
    }

    /// Cumulative count of permits granted (batches started).
    pub fn total_batches_started(&self) -> u64 {
        self.total_batches_started.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Barrier;

    // ─── Task 1.6: Unit tests for PartitionGate::new ───────────────────────

    #[test]
    fn new_with_1_cpu_thread() {
        let gate = PartitionGate::new(1);
        // floor(1 * 1.5) = 1
        assert_eq!(gate.max_permits(), 1);
    }

    #[test]
    fn new_with_2_cpu_threads() {
        let gate = PartitionGate::new(2);
        // floor(2 * 1.5) = 3
        assert_eq!(gate.max_permits(), 3);
    }

    #[test]
    fn new_with_4_cpu_threads() {
        let gate = PartitionGate::new(4);
        // floor(4 * 1.5) = 6
        assert_eq!(gate.max_permits(), 6);
    }

    #[test]
    fn new_with_8_cpu_threads() {
        let gate = PartitionGate::new(8);
        // floor(8 * 1.5) = 12
        assert_eq!(gate.max_permits(), 12);
    }

    #[test]
    fn new_with_odd_cpu_threads() {
        let gate = PartitionGate::new(3);
        // floor(3 * 1.5) = floor(4.5) = 4
        assert_eq!(gate.max_permits(), 4);
    }

    #[test]
    fn new_with_large_cpu_threads() {
        let gate = PartitionGate::new(128);
        // floor(128 * 1.5) = 192
        assert_eq!(gate.max_permits(), 192);
    }

    #[test]
    fn new_minimum_permits_is_one() {
        // Even with 0 cpu_threads, max(1) ensures at least 1 permit
        let gate = PartitionGate::new(0);
        assert_eq!(gate.max_permits(), 1);
    }

    #[test]
    fn new_initial_counters_are_zero() {
        let gate = PartitionGate::new(4);
        assert_eq!(gate.active_permits(), 0);
        assert_eq!(gate.total_wait_duration_ms(), 0);
        assert_eq!(gate.total_batches_started(), 0);
    }

    // ─── Task 1.7: Unit tests for acquire/release behavior ─────────────────

    #[tokio::test]
    async fn acquire_consumes_permit() {
        let gate = PartitionGate::new(4); // 6 permits
        assert_eq!(gate.active_permits(), 0);

        let permit = gate.acquire().await;
        assert_eq!(gate.active_permits(), 1);
        assert_eq!(gate.total_batches_started(), 1);

        drop(permit);
        assert_eq!(gate.active_permits(), 0);
    }

    #[tokio::test]
    async fn acquire_multiple_permits() {
        let gate = PartitionGate::new(2); // 3 permits
        let p1 = gate.acquire().await;
        let p2 = gate.acquire().await;
        let p3 = gate.acquire().await;

        assert_eq!(gate.active_permits(), 3);
        assert_eq!(gate.total_batches_started(), 3);

        drop(p1);
        assert_eq!(gate.active_permits(), 2);

        drop(p2);
        assert_eq!(gate.active_permits(), 1);

        drop(p3);
        assert_eq!(gate.active_permits(), 0);
    }

    #[tokio::test]
    async fn release_on_drop_restores_permit() {
        let gate = PartitionGate::new(1); // 1 permit

        {
            let _permit = gate.acquire().await;
            assert_eq!(gate.active_permits(), 1);
        }
        // permit dropped here
        assert_eq!(gate.active_permits(), 0);
    }

    // ─── Task 1.8: Unit test for blocking behavior ─────────────────────────

    #[tokio::test]
    async fn acquire_blocks_when_permits_exhausted() {
        let gate = Arc::new(PartitionGate::new(1)); // 1 permit
        let barrier = Arc::new(Barrier::new(2));

        // Acquire the only permit
        let permit = gate.acquire().await;
        assert_eq!(gate.active_permits(), 1);

        let gate_clone = Arc::clone(&gate);
        let barrier_clone = Arc::clone(&barrier);

        // Spawn a task that tries to acquire — it should block
        let handle = tokio::spawn(async move {
            // Signal that we're about to try acquiring
            barrier_clone.wait().await;
            // This will block until the permit is released
            let _permit = gate_clone.acquire().await;
            // If we get here, the permit was released
            true
        });

        // Wait for the spawned task to be ready
        barrier.wait().await;

        // Give the spawned task a moment to attempt acquisition
        tokio::task::yield_now().await;

        // The spawned task should still be pending (blocked)
        assert!(!handle.is_finished());

        // Release the permit — this should unblock the spawned task
        drop(permit);

        // The spawned task should now complete
        let result = handle.await.unwrap();
        assert!(result);
        assert_eq!(gate.total_batches_started(), 2);
    }

    // ─── Task 6: Property-Based Tests ──────────────────────────────────────

    mod prop_tests {
        use super::*;
        use proptest::prelude::*;

        /// **Validates: Requirements 1.2**
        /// P1: For any cpu_threads in [1, 256], max_permits == floor(cpu_threads × 1.5).max(1)
        proptest! {
            #[test]
            fn capacity_formula(cpu_threads in 1u32..=256u32) {
                let gate = PartitionGate::new(cpu_threads as usize);
                let expected = ((cpu_threads as f64) * 1.5).floor() as u32;
                let expected = expected.max(1);
                prop_assert_eq!(gate.max_permits(), expected);
            }
        }

        /// **Validates: Requirements 3.1**
        /// P2: After acquiring and releasing N permits, available_permits returns to max_permits
        proptest! {
            #[test]
            fn acquire_release_round_trip(n in 1u32..=10u32) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let gate = PartitionGate::new(16); // 24 permits — enough for any n <= 10
                    let initial_active = gate.active_permits();
                    prop_assert_eq!(initial_active, 0);

                    let mut permits = Vec::new();
                    for _ in 0..n {
                        permits.push(gate.acquire().await);
                    }
                    prop_assert_eq!(gate.active_permits(), n);

                    drop(permits);
                    prop_assert_eq!(gate.active_permits(), 0);
                    Ok(())
                })?;
            }
        }

        /// **Validates: Requirements 8.2**
        /// P5: total_batches_started equals the total number of successful permit acquisitions
        proptest! {
            #[test]
            fn counter_equals_acquisitions(n in 1u32..=20u32) {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let gate = PartitionGate::new(32); // 48 permits — enough for any n <= 20
                    for _ in 0..n {
                        let permit = gate.acquire().await;
                        drop(permit);
                    }
                    prop_assert_eq!(gate.total_batches_started(), n as u64);
                    Ok(())
                })?;
            }
        }
    }

    // ─── Task 8: Integration Tests ─────────────────────────────────────────

    /// Task 8.1: stream_next acquires and releases permit per batch
    ///
    /// Simulates the stream_next pattern: acquire permit, do async work
    /// (batch production), drop permit. Verifies counters and active state.
    #[tokio::test]
    async fn stream_next_pattern_acquires_and_releases() {
        let gate = PartitionGate::new(4); // 6 permits
        assert_eq!(gate.active_permits(), 0);

        // Simulate first stream_next call
        let permit = gate.acquire().await;
        assert_eq!(gate.active_permits(), 1);
        assert_eq!(gate.total_batches_started(), 1);

        // Simulate batch production (some async work)
        tokio::task::yield_now().await;

        // Drop permit (simulates end of stream_next)
        drop(permit);
        assert_eq!(gate.active_permits(), 0);

        // Simulate second stream_next call
        let permit = gate.acquire().await;
        assert_eq!(gate.active_permits(), 1);
        assert_eq!(gate.total_batches_started(), 2);

        // Simulate batch production
        tokio::task::yield_now().await;

        drop(permit);
        assert_eq!(gate.active_permits(), 0);

        // Verify cumulative counter reflects both calls
        assert_eq!(gate.total_batches_started(), 2);
    }

    /// Task 8.2: concurrent stream_next calls bounded by max_permits
    ///
    /// Verifies that max_permits concurrent acquisitions succeed, but
    /// max_permits+1 blocks until a permit is released.
    #[tokio::test]
    async fn concurrent_calls_bounded_by_max_permits() {
        let gate = Arc::new(PartitionGate::new(2)); // 3 permits
        let mut permits = Vec::new();

        // Acquire all 3 permits
        for _ in 0..3 {
            permits.push(gate.acquire().await);
        }
        assert_eq!(gate.active_permits(), 3);

        // 4th acquire should block
        let gate_clone = Arc::clone(&gate);
        let handle = tokio::spawn(async move {
            gate_clone.acquire().await
        });

        // Give the spawned task time to attempt acquisition
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Should still be blocked (3 active, 4th waiting)
        assert!(!handle.is_finished());
        assert_eq!(gate.active_permits(), 3);

        // Release one permit — should unblock the 4th
        permits.pop();

        // The 4th task should now complete
        let _fourth = handle.await.unwrap();
        assert_eq!(gate.active_permits(), 3); // still 3 (2 original + 1 new)
        assert_eq!(gate.total_batches_started(), 4);
    }

    /// Task 8.3: cancelled query does not leak permits
    ///
    /// Verifies that if a cancellation token fires while a task is waiting
    /// for a permit, no permit is leaked.
    #[tokio::test]
    async fn cancelled_query_does_not_leak_permits() {
        use tokio_util::sync::CancellationToken;

        let gate = Arc::new(PartitionGate::new(1)); // 1 permit

        // Hold the only permit
        let _blocker = gate.acquire().await;
        assert_eq!(gate.active_permits(), 1);

        // Create a cancellation token
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let gate_clone = Arc::clone(&gate);

        // Spawn a task that tries to acquire with cancellation
        // (mirrors the pattern in api::stream_next)
        let handle = tokio::spawn(async move {
            let result: Result<Option<OwnedSemaphorePermit>, String> =
                crate::cancellation::cancellable_or(
                    Some(&token_clone),
                    None,
                    async { Ok::<_, std::convert::Infallible>(Some(gate_clone.acquire().await)) },
                )
                .await;
            result
        });

        // Give the task time to start waiting
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Cancel the token
        token.cancel();

        // The task should complete with cancellation (Ok(None) — the sentinel)
        let result = handle.await.unwrap();
        assert!(matches!(result, Ok(None)));

        // No permit was acquired — only the blocker holds one
        assert_eq!(gate.active_permits(), 1);

        // Release the blocker
        drop(_blocker);
        assert_eq!(gate.active_permits(), 0);

        // Only 1 batch was started (the blocker), not the cancelled one
        assert_eq!(gate.total_batches_started(), 1);
    }

    /// Task 8.4: df_stats reports correct partition gate values
    ///
    /// Verifies that pack_partition_gate correctly reports the gate's state
    /// at various points in its lifecycle.
    #[tokio::test]
    async fn stats_reports_correct_partition_gate_values() {
        use crate::stats::pack_partition_gate;

        let gate = PartitionGate::new(4); // 6 permits

        // Initial state
        let stats = pack_partition_gate(&gate);
        assert_eq!(stats.max_permits, 6);
        assert_eq!(stats.active_permits, 0);
        assert_eq!(stats.total_wait_duration_ms, 0);
        assert_eq!(stats.total_batches_started, 0);

        // After acquiring a permit
        let permit = gate.acquire().await;
        let stats = pack_partition_gate(&gate);
        assert_eq!(stats.max_permits, 6);
        assert_eq!(stats.active_permits, 1);
        assert_eq!(stats.total_batches_started, 1);

        // After releasing
        drop(permit);
        let stats = pack_partition_gate(&gate);
        assert_eq!(stats.active_permits, 0);
        assert_eq!(stats.total_batches_started, 1); // cumulative — doesn't decrease
    }
}
